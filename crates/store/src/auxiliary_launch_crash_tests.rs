//! Selected auxiliary launch transaction boundaries across real process exit.
use super::transaction_cancellation::{auxiliary_origin_fixture, snapshot};
use super::*;
use std::path::Path;

fn audit(id: &str) -> EffectAudit<'_> {
    EffectAudit {
        id,
        actor_id: "runtime",
        correlation_id: "crash-launch",
        now_ms: 100,
    }
}

async fn crash_launch_transition(mode: &str, control: &Path) -> ! {
    let (operation, boundary) = mode.split_once(':').unwrap();
    let (root, store, input, host) = auxiliary_origin_fixture("admit").await;
    let binding = AuxiliaryLaunchBinding {
        launch_sha256: "c".repeat(64),
        authority_id: host.authority_id.clone(),
        receipt_root: "/private/receipts".into(),
        wrapper_path: "/private/wrapper".into(),
        wrapper_sha256: "e".repeat(64),
    };
    let process = ExecutionProcess {
        platform: host.platform.clone(),
        host_id: host.host_id.clone(),
        boot_id: host.boot_id.clone(),
        pid: 123,
        start_marker: "macos-start-us:1:2".into(),
    };
    store
        .prepare_auxiliary_with_host(&input, &host, audit("launch-crash-prepare"))
        .await
        .unwrap();
    if matches!(operation, "release" | "settle" | "unreleased") {
        assert!(
            store
                .bind_auxiliary_launch(&input, &binding, audit("launch-crash-bind"))
                .await
                .unwrap()
        );
    }
    if operation == "settle" {
        assert!(
            store
                .authorize_auxiliary_release(
                    &input,
                    &binding,
                    &process,
                    audit("launch-crash-release")
                )
                .await
                .unwrap()
        );
    }
    let observation = if operation == "unreleased" {
        "never_released"
    } else {
        "owned_group_settled"
    };
    let proof = serde_json::json!({"version":1,"invocation_id":input.invocation_id,"authority_id":binding.authority_id,"boot_generation":input.boot_generation,"launch_sha256":binding.launch_sha256,"process":{"host":{"platform":process.platform,"host_id":process.host_id,"boot_id":process.boot_id},"pid":process.pid,"start_marker":process.start_marker},"observation":observation}).to_string();
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
    match operation {
        "bind" => assert!(
            store
                .bind_auxiliary_launch(&input, &binding, audit("launch-crash-target"))
                .await
                .unwrap()
        ),
        "release" => assert!(
            store
                .authorize_auxiliary_release(
                    &input,
                    &binding,
                    &process,
                    audit("launch-crash-target")
                )
                .await
                .unwrap()
        ),
        "settle" => store
            .settle_auxiliary_proof(
                &input,
                &binding,
                &process,
                2,
                &proof,
                audit("launch-crash-target"),
            )
            .await
            .unwrap(),
        "unreleased" => store
            .settle_auxiliary_unreleased_proof(
                &input,
                &binding,
                2,
                &proof,
                audit("launch-crash-target"),
            )
            .await
            .unwrap(),
        "abandon" => store
            .abandon_auxiliary_before_launch(&input, audit("launch-crash-target"))
            .await
            .unwrap(),
        "unbound" => store
            .settle_auxiliary(&input, audit("launch-crash-target"))
            .await
            .unwrap(),
        _ => panic!("unknown launch transition"),
    }
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
    const MODE: &str = "CONDUCTOR_LAUNCH_TRANSITION_CRASH_MODE";
    const CONTROL: &str = "CONDUCTOR_LAUNCH_TRANSITION_CRASH_CONTROL";
    if let Ok(mode) = std::env::var(MODE) {
        crash_launch_transition(
            &mode,
            &std::path::PathBuf::from(std::env::var(CONTROL).unwrap()),
        )
        .await;
    }
    for operation in [
        "bind",
        "release",
        "settle",
        "unreleased",
        "abandon",
        "unbound",
    ] {
        for boundary in ["before", "after"] {
            let mode = format!("{operation}:{boundary}");
            let control = tempfile::tempdir().unwrap();
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "auxiliary_launch_crash_tests::launch_transition_survives_abrupt_exit_on_both_sides",
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
