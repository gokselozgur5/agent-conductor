use conductor_runtime::{
    BrokerError, BrokerOperation, BrokerRequest, BrokerScope, FileBrokerGate, MAX_BROKER_FRAME,
    continuous_millis,
};
use std::{
    fs,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

fn request() -> BrokerRequest {
    BrokerRequest {
        version: 1,
        sequence: 1,
        scope: BrokerScope {
            authority_id: "a".repeat(64),
            boot_generation: 1,
            invocation_id: uuid::Uuid::now_v7().to_string(),
            task_id: "task".into(),
            attempt_id: "attempt".into(),
            lease_epoch: 1,
        },
        operation: BrokerOperation::WriteText {
            path: "answer.txt".into(),
            expected_sha256: None,
            contents: "private fixture contents".into(),
        },
    }
}
fn gate(request: &BrokerRequest) -> FileBrokerGate {
    FileBrokerGate::new(request.scope.clone(), continuous_millis().unwrap() + 60_000).unwrap()
}
#[test]
fn strict_protocol_refuses_paths_versions_unknown_fields_and_large_content() {
    let input = request();
    let original = serde_json::to_value(&input).unwrap();
    assert!(BrokerRequest::decode(&serde_json::to_vec(&original).unwrap()).is_ok());
    for path in [
        "/outside",
        "../outside",
        "a/../b",
        "a//b",
        "./a",
        "a/",
        ".git/config",
        "a\\b",
        "a\nb",
        "",
    ] {
        let mut value = original.clone();
        value["operation"]["path"] = path.into();
        assert!(
            BrokerRequest::decode(&serde_json::to_vec(&value).unwrap()).is_err(),
            "{path:?}"
        );
    }
    for (key, value) in [
        ("version", 2.into()),
        ("sequence", 0.into()),
        ("unknown", "ignored?".into()),
    ] {
        let mut json = original.clone();
        json[key] = value;
        assert!(BrokerRequest::decode(&serde_json::to_vec(&json).unwrap()).is_err());
    }
    let mut json = original.clone();
    json["operation"]["command"] = "arbitrary".into();
    assert!(BrokerRequest::decode(&serde_json::to_vec(&json).unwrap()).is_err());
    let mut json = original;
    json["operation"]["contents"] = "x".repeat(256 * 1024 + 1).into();
    assert!(BrokerRequest::decode(&serde_json::to_vec(&json).unwrap()).is_err());
    assert!(BrokerRequest::decode(&vec![b' '; MAX_BROKER_FRAME + 1]).is_err());
}
#[test]
fn admission_precedes_effect_and_replay_or_wrong_scope_never_executes() {
    let request = request();
    let gate = gate(&request);
    let root = tempfile::tempdir().unwrap();
    let intent = root.path().join("intent");
    let output = root.path().join("output");
    gate.execute(
        &request,
        |record| {
            assert_eq!(record.sequence, 1);
            assert_eq!(record.request_sha256.len(), 64);
            assert!(!output.exists());
            fs::write(&intent, &record.request_sha256).unwrap();
            Ok(())
        },
        |_| {
            assert!(intent.exists());
            fs::write(&output, "accepted").unwrap();
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(fs::read_to_string(output).unwrap(), "accepted");
    assert_eq!(
        gate.execute(&request, |_| panic!("replayed intent"), |_| Ok(())),
        Err(BrokerError::Refused)
    );
    let mut changed = request;
    changed.sequence = 2;
    changed.scope.lease_epoch += 1;
    assert_eq!(
        gate.execute(&changed, |_| panic!("stale scope"), |_| Ok(())),
        Err(BrokerError::Refused)
    );
    assert_eq!(gate.revoke(), Ok(1));
    changed.scope.lease_epoch -= 1;
    assert_eq!(
        gate.execute(&changed, |_| panic!("revoked intent"), |_| Ok(())),
        Err(BrokerError::Revoked)
    );
    assert_eq!(gate.revoke(), Ok(1));
}
#[test]
fn failed_admission_or_partial_write_never_returns_clean_revocation() {
    let request = request();
    let gate = gate(&request);
    assert_eq!(
        gate.execute(
            &request,
            |_| Err(BrokerError::Admission),
            |_| panic!("uncommitted effect")
        ),
        Err::<(), _>(BrokerError::Admission)
    );
    assert_eq!(gate.revoke(), Err(BrokerError::Uncertain));
    let gate =
        FileBrokerGate::new(request.scope.clone(), continuous_millis().unwrap() + 60_000).unwrap();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("retained");
    assert_eq!(
        gate.execute(
            &request,
            |_| Ok(()),
            |_| {
                fs::write(&path, "partial").unwrap();
                Err::<(), _>(BrokerError::Uncertain)
            }
        ),
        Err(BrokerError::Uncertain)
    );
    assert_eq!(fs::read_to_string(path).unwrap(), "partial");
    assert_eq!(gate.revoke(), Err(BrokerError::Uncertain));
    assert_eq!(
        gate.execute(&request, |_| panic!("uncertain replay"), |_| Ok(())),
        Err(BrokerError::Revoked)
    );
}
#[test]
fn revoke_during_admission_prevents_apply_and_keeps_committed_intent_uncertain() {
    let request = request();
    let gate = Arc::new(gate(&request));
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    thread::scope(|scope| {
        let executor = Arc::clone(&gate);
        let operation = scope.spawn(move || {
            executor.execute(
                &request,
                |_| {
                    entered.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                },
                |_| panic!("revoked before apply"),
            )
        });
        entry.recv_timeout(Duration::from_secs(5)).unwrap();
        gate.request_revocation();
        let revoker = Arc::clone(&gate);
        let revocation = scope.spawn(move || revoker.revoke());
        release.send(()).unwrap();
        let result: Result<(), _> = operation.join().unwrap();
        assert!(matches!(result, Err(BrokerError::Revoked)));
        assert_eq!(revocation.join().unwrap(), Err(BrokerError::Uncertain));
    });
}
#[test]
fn revoke_waits_for_inflight_write_and_denies_queued_work() {
    let request = request();
    let gate = Arc::new(gate(&request));
    let mut queued = BrokerRequest::decode(&serde_json::to_vec(&request).unwrap()).unwrap();
    queued.sequence = 2;

    let root = tempfile::tempdir().unwrap();
    let output = root.path().join("output");
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    thread::scope(|scope| {
        let executor = Arc::clone(&gate);
        let path = output.clone();
        let operation = scope.spawn(move || {
            executor.execute(
                &request,
                |_| Ok(()),
                |_| {
                    entered.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(5)).unwrap();
                    fs::write(path, "completed before revocation acknowledgement").unwrap();
                    Ok(())
                },
            )
        });
        entry.recv_timeout(Duration::from_secs(5)).unwrap();
        gate.request_revocation();
        assert_eq!(
            gate.execute(
                &queued,
                |_| panic!("queued admission after revocation"),
                |_| Ok(())
            ),
            Err(BrokerError::Revoked)
        );

        let revoker = Arc::clone(&gate);
        let (finished, finish) = mpsc::channel();
        let revocation = scope.spawn(move || {
            let result = revoker.revoke();
            finished.send(()).unwrap();
            result
        });
        assert!(finish.recv_timeout(Duration::from_millis(20)).is_err());
        assert!(!output.exists());
        release.send(()).unwrap();
        assert_eq!(operation.join().unwrap(), Ok(()));
        assert_eq!(revocation.join().unwrap(), Ok(1));
        assert!(output.exists());
    });
}
#[test]
fn panic_or_expiry_cannot_be_converted_to_clean_admission() {
    let request = request();
    let gate = gate(&request);
    let result = std::panic::catch_unwind(|| {
        gate.execute(
            &request,
            |_| Ok(()),
            |_| -> Result<(), BrokerError> { panic!("simulated interrupted port") },
        )
    });
    assert!(result.is_err());
    assert_eq!(gate.revoke(), Err(BrokerError::Uncertain));
    assert!(matches!(
        FileBrokerGate::new(request.scope.clone(), 0),
        Err(BrokerError::Revoked)
    ));
}

#[test]
fn concurrent_duplicate_requests_admit_and_apply_exactly_once() {
    use std::sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
    };
    let request = request();
    let gate = gate(&request);
    let start = Barrier::new(3);
    let admitted = AtomicUsize::new(0);
    let applied = AtomicUsize::new(0);
    thread::scope(|scope| {
        let run = || {
            start.wait();
            gate.execute(
                &request,
                |_| {
                    admitted.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                |_| {
                    applied.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            )
        };
        let one = scope.spawn(run);
        let two = scope.spawn(run);
        start.wait();
        let results = [one.join().unwrap(), two.join().unwrap()];
        assert_eq!(results.iter().filter(|v| v.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|v| **v == Err(BrokerError::Refused))
                .count(),
            1
        );
    });
    assert_eq!(admitted.load(Ordering::SeqCst), 1);
    assert_eq!(applied.load(Ordering::SeqCst), 1);
}

#[test]
fn revocation_waits_for_successful_result_persistence() {
    let request = request();
    let gate = Arc::new(gate(&request));
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    thread::scope(|scope| {
        let executor = Arc::clone(&gate);
        let operation = scope.spawn(move || {
            executor.execute_recorded(
                &request,
                |_| Ok(()),
                |_| Ok("applied"),
                |_, value| {
                    assert_eq!(*value, "applied");
                    entered.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                },
            )
        });
        entry.recv_timeout(Duration::from_secs(5)).unwrap();
        gate.request_revocation();
        let revoker = Arc::clone(&gate);
        let (finished, finish) = mpsc::channel();
        let revocation = scope.spawn(move || {
            let result = revoker.revoke();
            finished.send(()).unwrap();
            result
        });
        assert!(finish.recv_timeout(Duration::from_millis(20)).is_err());
        release.send(()).unwrap();
        assert_eq!(operation.join().unwrap(), Ok("applied"));
        assert_eq!(revocation.join().unwrap(), Ok(1));
    });
}
