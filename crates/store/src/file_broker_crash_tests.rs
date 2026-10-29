//! Real child exits at SQLite/file boundaries; no Rust unwinding on crash cuts.
use super::file_broker_tests::{audit, fixture};
use super::transaction_cancellation::snapshot;
use super::*;
use conductor_runtime::{
    BrokerError, BrokerFileResult, BrokerFilesystem, BrokerOperation, BrokerRequest, BrokerScope,
    continuous_millis,
};
use sha2::{Digest, Sha256};
use std::path::Path;

async fn save(store: &Store, path: &Path) {
    let state = snapshot(&mut *store.inner.writer.lock().await).await;
    std::fs::write(path, serde_json::to_vec(&state).unwrap()).unwrap();
}
async fn exit_on_write(store: &Store, table: &'static str) {
    store
        .inner
        .writer
        .lock()
        .await
        .lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |update| {
            if update.table == table {
                std::process::exit(78);
            }
        });
}
async fn child(stage: String, control: std::path::PathBuf) -> ! {
    let (root, store, b) = fixture().await;
    let files = root.path().join("files");
    std::fs::create_dir(&files).unwrap();
    let request = BrokerRequest {
        version: 1,
        sequence: 1,
        scope: BrokerScope {
            authority_id: b.authority_id.clone(),
            boot_generation: 2,
            invocation_id: b.invocation_id.clone(),
            task_id: b.task_id.clone(),
            attempt_id: b.attempt_id.clone(),
            lease_epoch: 7,
        },
        operation: BrokerOperation::WriteText {
            path: "answer".into(),
            contents: "bytes survive abrupt exit".into(),
            expected_sha256: None,
        },
    };
    std::fs::write(
        control.join("database"),
        root.path().join("state.sqlite3").to_str().unwrap(),
    )
    .unwrap();
    std::fs::write(
        control.join("binding.json"),
        serde_json::to_vec(&b).unwrap(),
    )
    .unwrap();
    std::fs::write(
        control.join("request.json"),
        serde_json::to_vec(&request).unwrap(),
    )
    .unwrap();
    save(&store, &control.join("initial.json")).await;
    save(&store, &control.join("before.json")).await;
    if stage == "registration_write" {
        exit_on_write(&store, "file_brokers").await;
    }
    assert!(
        store
            .register_file_broker(&b, audit("register"))
            .await
            .unwrap()
    );
    if stage == "registered" {
        std::process::exit(78);
    }
    save(&store, &control.join("before.json")).await;
    if stage == "admission_write" {
        exit_on_write(&store, "file_operations").await;
    }
    let handle = tokio::runtime::Handle::current();
    let owned = store.clone();
    let binding = b.clone();
    let mode = stage.clone();
    let directory = control.clone();
    let completed = tokio::task::spawn_blocking(move || {
        let broker = BrokerFilesystem::new(
            &files.canonicalize().unwrap(),
            request.scope.clone(),
            continuous_millis().unwrap() + 60_000,
        )
        .unwrap();
        broker
            .execute_recorded(
                &request,
                |intent| {
                    assert!(
                        handle
                            .block_on(owned.admit_file_operation(
                                &binding,
                                1,
                                &intent.request_sha256,
                                audit("admit")
                            ))
                            .unwrap()
                    );
                    if mode == "admitted" {
                        std::process::exit(78);
                    }
                    Ok(())
                },
                |intent, result| {
                    assert_eq!(
                        std::fs::read_to_string(files.join("answer")).unwrap(),
                        "bytes survive abrupt exit"
                    );
                    handle.block_on(save(&owned, &directory.join("before.json")));
                    if mode == "filesystem_written" {
                        std::process::exit(78);
                    }
                    if mode == "completion_write" {
                        handle.block_on(exit_on_write(&owned, "file_operations"));
                    }
                    let hash = match result {
                        BrokerFileResult::Text { sha256, .. }
                        | BrokerFileResult::Written { sha256 } => sha256,
                    };
                    assert!(
                        handle
                            .block_on(owned.finish_file_operation(
                                &binding,
                                1,
                                &intent.request_sha256,
                                hash,
                                audit("finish")
                            ))
                            .unwrap()
                    );
                    Ok::<(), BrokerError>(())
                },
            )
            .unwrap();
        if mode == "completed" {
            std::process::exit(78);
        }
        broker.revoke().unwrap()
    })
    .await
    .unwrap();
    assert_eq!(completed, 1);
    save(&store, &control.join("before.json")).await;
    if stage == "closure_write" {
        exit_on_write(&store, "file_brokers").await;
    }
    assert!(
        store
            .close_file_broker(&b, 1, audit("close"))
            .await
            .unwrap()
    );
    assert_eq!(stage, "closed");
    std::process::exit(78);
}

#[test]
fn broker_crash_child() {
    let Ok(stage) = std::env::var("CONDUCTOR_BROKER_CRASH_STAGE") else {
        return;
    };
    let control =
        std::path::PathBuf::from(std::env::var_os("CONDUCTOR_BROKER_CRASH_CONTROL").unwrap());
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(child(stage, control));
}

async fn verify(stage: &str, control: &Path) {
    let path = std::fs::read_to_string(control.join("database")).unwrap();
    let store = Store::open(&path).await.unwrap();
    let b: FileBrokerBinding =
        serde_json::from_slice(&std::fs::read(control.join("binding.json")).unwrap()).unwrap();
    let request = std::fs::read(control.join("request.json")).unwrap();
    let hash = format!("{:x}", Sha256::digest(request));
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(store.readers())
        .await
        .unwrap();
    assert_eq!(integrity, "ok");
    let initial: Vec<(String, String)> =
        serde_json::from_slice(&std::fs::read(control.join("initial.json")).unwrap()).unwrap();
    let actual = snapshot(&mut *store.inner.writer.lock().await).await;
    if stage.ends_with("_write") {
        let before: Vec<(String, String)> =
            serde_json::from_slice(&std::fs::read(control.join("before.json")).unwrap()).unwrap();
        assert_eq!(actual, before, "{stage} left partial transaction");
    }
    for row in &initial {
        if !matches!(
            row.0.as_str(),
            "file_brokers" | "file_operations" | "audit_events"
        ) {
            assert_eq!(
                actual.iter().find(|v| v.0 == row.0),
                Some(row),
                "{stage} changed {}",
                row.0
            );
        }
    }
    let written = matches!(
        stage,
        "filesystem_written" | "completion_write" | "completed" | "closure_write" | "closed"
    );
    let answer = Path::new(&path).parent().unwrap().join("files/answer");
    if written {
        assert_eq!(
            std::fs::read_to_string(&answer).unwrap(),
            "bytes survive abrupt exit"
        );
    } else {
        assert!(!answer.exists());
    }
    let registered = stage != "registration_write";
    let admitted = !matches!(
        stage,
        "registration_write" | "registered" | "admission_write"
    );
    let completed = matches!(stage, "completed" | "closure_write" | "closed");
    let closed = stage == "closed";
    let audits: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE kind LIKE 'file_broker.%'")
            .fetch_one(store.readers())
            .await
            .unwrap();
    assert_eq!(
        audits,
        i64::from(registered) + i64::from(admitted) + i64::from(completed) + i64::from(closed)
    );
    if !registered {
        assert!(store.file_broker_status(&b).await.is_err());
        assert!(
            store
                .register_file_broker(&b, audit("fresh-register"))
                .await
                .unwrap()
        );
    } else {
        assert_eq!(
            store.file_broker_status(&b).await.unwrap(),
            FileBrokerStatus {
                closed,
                admitted: i64::from(admitted),
                completed: i64::from(completed)
            }
        );
        assert!(
            !store
                .register_file_broker(&b, audit("re-register"))
                .await
                .unwrap()
        );
    }
    if admitted {
        let replay = store
            .admit_file_operation(&b, 1, &hash, audit("replay"))
            .await;
        if closed {
            assert!(replay.is_err());
        } else {
            assert!(!replay.unwrap());
        }
        if !completed {
            assert!(
                store
                    .close_file_broker(&b, 1, audit("false-close"))
                    .await
                    .is_err()
            );
            assert!(
                store
                    .admit_file_operation(&b, 2, &hash, audit("skip-pending"))
                    .await
                    .is_err()
            );
        }
    } else {
        assert!(
            store
                .admit_file_operation(&b, 1, &hash, audit("fresh-admit"))
                .await
                .unwrap()
        );
    }
    let before_restart = snapshot(&mut *store.inner.writer.lock().await).await;
    let reopened = Store::open(&path).await.unwrap();
    assert_eq!(
        snapshot(&mut *reopened.inner.writer.lock().await).await,
        before_restart
    );
    reopened
        .start_reserved_runtime(101, &b.authority_id, 3)
        .await
        .unwrap();
    assert!(
        reopened
            .admit_file_operation(&b, 2, &hash, audit("old-admit"))
            .await
            .is_err()
    );
    assert!(
        reopened
            .close_file_broker(&b, i64::from(completed), audit("old-close"))
            .await
            .is_err()
    );
    if written {
        assert_eq!(
            std::fs::read_to_string(answer).unwrap(),
            "bytes survive abrupt exit"
        );
    }
}

#[tokio::test]
async fn broker_abrupt_process_exit_matrix() {
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    for stage in [
        "registration_write",
        "registered",
        "admission_write",
        "admitted",
        "filesystem_written",
        "completion_write",
        "completed",
        "closure_write",
        "closed",
    ] {
        let control = tempfile::tempdir().unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "file_broker_crash_tests::broker_crash_child",
                "--nocapture",
            ])
            .env("CONDUCTOR_BROKER_CRASH_STAGE", stage)
            .env("CONDUCTOR_BROKER_CRASH_CONTROL", control.path())
            .env("TMPDIR", control.path())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let start = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if start.elapsed() > Duration::from_secs(20) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("{stage} crash deadline");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(status.code(), Some(78), "{stage}");
        verify(stage, control.path()).await;
    }
}
