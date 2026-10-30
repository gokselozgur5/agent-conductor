//! Selected broker launch transaction process-death boundaries.
use super::file_broker_tests::{audit, fixture};
use super::transaction_cancellation::snapshot;
use super::*;
use std::path::Path;

async fn crash_launch_transition(mode: &str, control: &Path) -> ! {
    let (operation, boundary) = mode.split_once(':').unwrap();
    let (root, store, binding) = fixture().await;
    let launch = FileBrokerLaunchBinding {
        launch_sha256: "a".repeat(64),
        wrapper_path: "/approved/runner".into(),
        wrapper_sha256: "b".repeat(64),
        receipt_root: "/private/receipts".into(),
        receipt_root_identity_sha256: "c".repeat(64),
        platform: "linux".into(),
        host_id: "d".repeat(64),
        boot_id: "e".repeat(64),
    };
    let process = ExecutionProcess {
        platform: launch.platform.clone(),
        host_id: launch.host_id.clone(),
        boot_id: launch.boot_id.clone(),
        pid: 42,
        start_marker: "linux-start-ticks:123".into(),
    };
    let owner = FileBrokerOwner {
        receipt_root: Some(launch.receipt_root.clone()),
        launch_sha256: launch.launch_sha256.clone(),
        receipt_root_identity_sha256: launch.receipt_root_identity_sha256.clone(),
        process: process.clone(),
    };
    sqlx::query("UPDATE executions SET state='prepared',wrapper_identity=NULL,process_json=NULL")
        .execute(&mut *store.inner.writer.lock().await)
        .await
        .unwrap();
    if operation != "prepare" {
        assert!(
            store
                .prepare_file_broker_launch(&binding, &launch, audit("crash-prepare"))
                .await
                .unwrap()
        );
        sqlx::query("UPDATE executions SET state='started',wrapper_identity=expected_wrapper_identity,process_json=?")
            .bind(serde_json::to_string(&process).unwrap()).execute(&mut *store.inner.writer.lock().await).await.unwrap();
        if operation != "unregistered" {
            assert!(
                store
                    .register_file_broker(&binding, audit("crash-register"))
                    .await
                    .unwrap()
            );
        }
    }
    let proof = UnreleasedBrokerLaunchProof {
        launch: launch.clone(),
        process,
        shutdown_sha256: "f".repeat(64),
    };
    let before = snapshot(&mut *store.inner.writer.lock().await).await;
    std::fs::write(
        control.join("database"),
        root.path().join("state.sqlite3").to_str().unwrap(),
    )
    .unwrap();
    std::fs::write(
        control.join("before.json"),
        serde_json::to_vec(&before).unwrap(),
    )
    .unwrap();
    if boundary == "before" {
        store
            .inner
            .writer
            .lock()
            .await
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(|update| {
                if update.table == "audit_events" {
                    std::process::exit(81);
                }
            });
    }
    let changed = match operation {
        "prepare" => store
            .prepare_file_broker_launch(&binding, &launch, audit("crash-target"))
            .await
            .unwrap(),
        "release" => store
            .authorize_file_broker_release(&binding, &owner, audit("crash-target"))
            .await
            .unwrap(),
        "unregistered" => store
            .recover_unregistered_broker_launch(&binding, &proof, 2, audit("crash-target"))
            .await
            .unwrap(),
        "unowned" => store
            .recover_unowned_broker_launch(&binding, &proof, 2, audit("crash-target"))
            .await
            .unwrap(),
        _ => panic!("unknown broker launch operation"),
    };
    assert!(changed);
    assert_eq!(boundary, "after", "pre-commit write not reached");
    let after = snapshot(&mut *store.inner.writer.lock().await).await;
    assert_ne!(before, after);
    std::fs::write(
        control.join("after.json"),
        serde_json::to_vec(&after).unwrap(),
    )
    .unwrap();
    std::process::exit(82);
}

#[tokio::test]
async fn launch_transition_survives_abrupt_exit_on_both_sides() {
    const MODE: &str = "CONDUCTOR_BROKER_LAUNCH_TRANSITION_CRASH_MODE";
    const CONTROL: &str = "CONDUCTOR_BROKER_LAUNCH_TRANSITION_CRASH_CONTROL";
    if let Ok(mode) = std::env::var(MODE) {
        crash_launch_transition(
            &mode,
            &std::path::PathBuf::from(std::env::var(CONTROL).unwrap()),
        )
        .await;
    }
    for operation in ["prepare", "release", "unregistered", "unowned"] {
        for boundary in ["before", "after"] {
            let mode = format!("{operation}:{boundary}");
            let control = tempfile::tempdir().unwrap();
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "broker_launch_crash_tests::launch_transition_survives_abrupt_exit_on_both_sides",
                    "--nocapture",
                ])
                .env(MODE, &mode)
                .env(CONTROL, control.path())
                .env("TMPDIR", control.path())
                .spawn()
                .unwrap();
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
            let status = loop {
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                if tokio::time::Instant::now() >= deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("launch transition crash deadline: {mode}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            };
            assert_eq!(
                status.code(),
                Some(if boundary == "before" { 81 } else { 82 }),
                "{mode}"
            );
            let store =
                Store::open(std::fs::read_to_string(control.path().join("database")).unwrap())
                    .await
                    .unwrap();
            let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
                .fetch_one(store.readers())
                .await
                .unwrap();
            assert_eq!(integrity, "ok");
            let expected: Vec<(String, String)> = serde_json::from_slice(
                &std::fs::read(control.path().join(if boundary == "before" {
                    "before.json"
                } else {
                    "after.json"
                }))
                .unwrap(),
            )
            .unwrap();
            assert_eq!(
                snapshot(&mut *store.inner.writer.lock().await).await,
                expected,
                "{mode}"
            );
            assert_eq!(
                snapshot(&mut store.readers().acquire().await.unwrap()).await,
                expected,
                "{mode}"
            );
        }
    }
}
