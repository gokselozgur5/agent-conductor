use super::file_broker_tests::{audit, fixture};
use super::transaction_cancellation::snapshot;
use super::*;
use sqlx::Executor;

fn owner() -> FileBrokerOwner {
    FileBrokerOwner {
        launch_sha256: "c".repeat(64),
        receipt_root_identity_sha256: "d".repeat(64),
        process: ExecutionProcess {
            platform: "linux".into(),
            host_id: "a".repeat(64),
            boot_id: "b".repeat(64),
            pid: 42,
            start_marker: "linux-start-ticks:123".into(),
        },
    }
}
#[tokio::test]
async fn broker_owner_is_fresh_exact_and_required_for_closure() {
    let (root, store, binding) = fixture().await;
    store
        .register_file_broker(&binding, audit("register"))
        .await
        .unwrap();
    let owner = owner();
    assert!(
        store
            .authorize_file_broker_release(&binding, &owner, audit("release"))
            .await
            .unwrap()
    );
    assert!(
        !store
            .authorize_file_broker_release(&binding, &owner, audit("replay"))
            .await
            .unwrap()
    );
    let mut changed = owner.clone();
    changed.process.pid += 1;
    assert!(
        store
            .authorize_file_broker_release(&binding, &changed, audit("changed"))
            .await
            .is_err()
    );
    assert!(
        store
            .close_file_broker(&binding, 0, audit("bypass"))
            .await
            .is_err()
    );
    assert!(
        store
            .close_owned_file_broker(&binding, &changed, 0, &"e".repeat(64), audit("wrong-owner"))
            .await
            .is_err()
    );
    store
        .admit_file_operation(&binding, 1, &"f".repeat(64), audit("admit"))
        .await
        .unwrap();
    assert!(
        store
            .close_owned_file_broker(&binding, &owner, 1, &"e".repeat(64), audit("pending"))
            .await
            .is_err()
    );
    store
        .finish_file_operation(
            &binding,
            1,
            &"f".repeat(64),
            &"a".repeat(64),
            audit("finish"),
        )
        .await
        .unwrap();
    assert!(
        store
            .close_owned_file_broker(&binding, &owner, 1, &"e".repeat(64), audit("close"))
            .await
            .unwrap()
    );
    assert!(
        !store
            .close_owned_file_broker(&binding, &owner, 1, &"e".repeat(64), audit("same-close"))
            .await
            .unwrap()
    );
    assert!(
        store
            .close_owned_file_broker(
                &binding,
                &owner,
                1,
                &"d".repeat(64),
                audit("changed-receipt")
            )
            .await
            .is_err()
    );
    let reopened = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    assert_eq!(reopened.file_broker_owner(&binding).await.unwrap(), owner);
    assert!(reopened.file_broker_status(&binding).await.unwrap().closed);
}
#[tokio::test]
async fn broker_owner_audit_failure_rolls_back_identity_and_receipt() {
    for kind in ["release_authorized", "closed"] {
        let (_root, store, binding) = fixture().await;
        store
            .register_file_broker(&binding, audit("register"))
            .await
            .unwrap();
        let owner = owner();
        if kind == "closed" {
            store
                .authorize_file_broker_release(&binding, &owner, audit("release"))
                .await
                .unwrap();
        }
        {
            let mut db = store.inner.writer.lock().await;
            db.execute(format!("CREATE TRIGGER reject_owner BEFORE INSERT ON audit_events WHEN NEW.kind='file_broker.{kind}' BEGIN SELECT RAISE(FAIL,'fixture'); END").as_str()).await.unwrap();
        }
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        let outcome = if kind == "closed" {
            store
                .close_owned_file_broker(&binding, &owner, 0, &"e".repeat(64), audit("close"))
                .await
        } else {
            store
                .authorize_file_broker_release(&binding, &owner, audit("release"))
                .await
        };
        assert!(outcome.is_err());
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            before
        );
    }
}

#[tokio::test]
async fn broker_recovery_requires_current_generation_exact_owner_and_complete_effects() {
    let (_root, store, binding) = fixture().await;
    store
        .register_file_broker(&binding, audit("register"))
        .await
        .unwrap();
    let owner = owner();
    store
        .authorize_file_broker_release(&binding, &owner, audit("release"))
        .await
        .unwrap();
    store
        .admit_file_operation(&binding, 1, &"f".repeat(64), audit("admit"))
        .await
        .unwrap();
    store
        .start_reserved_runtime(200, &binding.authority_id, 3)
        .await
        .unwrap();
    let before = snapshot(&mut *store.inner.writer.lock().await).await;
    for generation in [1, 2, 3, 4] {
        assert!(
            store
                .recover_owned_file_broker(
                    &binding,
                    &owner,
                    1,
                    &"e".repeat(64),
                    generation,
                    audit("pending")
                )
                .await
                .is_err()
        );
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            before
        );
    }
}
#[tokio::test]
async fn broker_recovery_audit_failure_and_identity_changes_preserve_open_state() {
    let (_root, store, binding) = fixture().await;
    store
        .register_file_broker(&binding, audit("register"))
        .await
        .unwrap();
    let owner = owner();
    store
        .authorize_file_broker_release(&binding, &owner, audit("release"))
        .await
        .unwrap();
    store
        .start_reserved_runtime(200, &binding.authority_id, 3)
        .await
        .unwrap();
    for mode in [
        "authority",
        "binding",
        "process",
        "receipt_root",
        "generation",
    ] {
        let mut changed_binding = binding.clone();
        let mut changed_owner = owner.clone();
        match mode {
            "authority" => changed_binding.authority_id = "f".repeat(64),
            "binding" => changed_binding.boot_generation = 1,
            "process" => changed_owner.process.pid += 1,
            "receipt_root" => changed_owner.receipt_root_identity_sha256 = "f".repeat(64),
            _ => {}
        }
        assert!(
            store
                .recover_owned_file_broker(
                    &changed_binding,
                    &changed_owner,
                    0,
                    &"e".repeat(64),
                    if mode == "generation" { 4 } else { 3 },
                    audit("changed")
                )
                .await
                .is_err()
        );
    }
    {
        let mut db = store.inner.writer.lock().await;
        db.execute("CREATE TRIGGER reject_recovery BEFORE INSERT ON audit_events WHEN NEW.kind='file_broker.recovered' BEGIN SELECT RAISE(FAIL,'fixture'); END").await.unwrap();
    }
    let before = snapshot(&mut *store.inner.writer.lock().await).await;
    assert!(
        store
            .recover_owned_file_broker(&binding, &owner, 0, &"e".repeat(64), 3, audit("fail-close"))
            .await
            .is_err()
    );
    assert_eq!(
        snapshot(&mut *store.inner.writer.lock().await).await,
        before
    );
    store
        .inner
        .writer
        .lock()
        .await
        .execute("DROP TRIGGER reject_recovery")
        .await
        .unwrap();
    assert!(
        store
            .recover_owned_file_broker(&binding, &owner, 0, &"e".repeat(64), 3, audit("close"))
            .await
            .unwrap()
    );
    assert!(
        !store
            .recover_owned_file_broker(&binding, &owner, 0, &"e".repeat(64), 3, audit("replay"))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn broker_recovery_audit_binds_both_generations_without_duplicate_event() {
    let (_root, store, binding) = fixture().await;
    store
        .register_file_broker(&binding, audit("register"))
        .await
        .unwrap();
    let owner = owner();
    store
        .authorize_file_broker_release(&binding, &owner, audit("release"))
        .await
        .unwrap();
    store
        .start_reserved_runtime(200, &binding.authority_id, 3)
        .await
        .unwrap();
    for id in ["recover", "replay"] {
        store
            .recover_owned_file_broker(&binding, &owner, 0, &"e".repeat(64), 3, audit(id))
            .await
            .unwrap();
    }
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT payload_json FROM audit_events WHERE kind='file_broker.recovered'",
    )
    .fetch_all(&store.inner.readers)
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    let payload: serde_json::Value = serde_json::from_str(&rows[0]).unwrap();
    assert_eq!(payload["origin_generation"], 2);
    assert_eq!(payload["recovery_generation"], 3);
    assert_eq!(payload["sequence"], 0);
}
