#![cfg(target_os = "macos")]
use conductor_runtime::{CheckDefinition, CheckError, CheckOutcome, execute_check, snapshot_paths};
use std::fs;
use tokio::sync::watch;

#[tokio::test]
async fn checks_use_fresh_bound_copies_and_cannot_modify_source() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("value"), "original").unwrap();
    let paths = vec!["value".into()];
    let manifest = snapshot_paths(&source, &root.path().join("baseline"), &paths).unwrap();
    let hash = manifest.sha256().unwrap();
    let (sender, receiver) = watch::channel(false);
    let check = CheckDefinition {
        id: "mutation-isolation".into(),
        timeout_ms: 5000,
        argv: vec![
            "/bin/sh".into(),
            "-c".into(),
            format!(
                "test \"$(cat value)\" = original && echo changed > value && ! /bin/sh -c 'echo escaped > \"{}/value\"'",
                source.display()
            ),
        ],
    };
    let receipt = execute_check(
        &source,
        &paths,
        &hash,
        &root.path().join("check1"),
        &check,
        &[],
        receiver.clone(),
    )
    .await
    .unwrap();
    assert_eq!(receipt.outcome, CheckOutcome::Passed);
    assert_eq!(receipt.source_sha256, hash);
    assert_eq!(
        fs::read_to_string(source.join("value")).unwrap(),
        "original"
    );
    let second = CheckDefinition {
        id: "fresh-copy".into(),
        argv: vec![
            "/bin/sh".into(),
            "-c".into(),
            "test \"$(cat value)\" = original".into(),
        ],
        ..check.clone()
    };
    let fresh = execute_check(
        &source,
        &paths,
        &hash,
        &root.path().join("check2"),
        &second,
        &[],
        receiver.clone(),
    )
    .await
    .unwrap();
    assert_eq!(fresh.outcome, CheckOutcome::Passed);
    assert_ne!(receipt.definition_sha256, fresh.definition_sha256);
    fs::write(source.join("value"), "tampered").unwrap();
    assert!(matches!(
        execute_check(
            &source,
            &paths,
            &hash,
            &root.path().join("check3"),
            &second,
            &[],
            receiver
        )
        .await,
        Err(CheckError::SourceChanged)
    ));
    drop(sender);
}

#[tokio::test]
async fn unsuccessful_checks_never_become_passing_evidence() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    let hash = snapshot_paths(&source, &root.path().join("baseline"), &[])
        .unwrap()
        .sha256()
        .unwrap();
    for (index, script, timeout_ms, cancelled, expected) in [
        (0, "exit 7", 5000, false, CheckOutcome::Failed),
        (1, "sleep 5", 100, false, CheckOutcome::TimedOut),
        (2, "exit 0", 5000, true, CheckOutcome::Cancelled),
    ] {
        let (_sender, receiver) = watch::channel(cancelled);
        let check = CheckDefinition {
            id: format!("check-{index}"),
            argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
            timeout_ms,
        };
        let receipt = execute_check(
            &source,
            &[],
            &hash,
            &root.path().join(format!("run{index}")),
            &check,
            &[],
            receiver,
        )
        .await
        .unwrap();
        assert_eq!(receipt.outcome, expected);
    }
}
