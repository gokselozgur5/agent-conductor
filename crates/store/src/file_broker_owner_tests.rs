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
