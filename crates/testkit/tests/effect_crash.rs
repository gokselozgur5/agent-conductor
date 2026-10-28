use std::process::Command;

use agent_conductor_store::{
    AuditAppend, CommitBatch, CommitResult, EffectIntentAppend, EffectStatus, Store,
};
use agent_conductor_testkit::{
    CrashPoint, FakeEffectRunner, FakeInventory, FakeResource, ReconcileDirective,
};
use tempfile::TempDir;

const TOKEN: &str = "effect-v1-test-token";

fn paths(directory: &TempDir) -> (String, String) {
    (
        directory
            .path()
            .join("control.sqlite3")
            .to_string_lossy()
            .into_owned(),
        directory
            .path()
            .join("external.sqlite3")
            .to_string_lossy()
            .into_owned(),
    )
}

fn intent_batch() -> CommitBatch {
    CommitBatch {
        actor_id: "system".into(),
        idempotency_key: "create-effect".into(),
        request_hash: "sha256:create-effect".into(),
        task: None,
        project: None,
        agent: None,
        new_task: None,
        attempt: None,
        transitions: vec![],
        audit: vec![AuditAppend {
            id: "audit-create-effect".into(),
            task_id: None,
            actor_id: "system".into(),
            kind: "effect.intent_created".into(),
            correlation_id: "correlation-test".into(),
            payload_json: format!(r#"{{"token":"{TOKEN}"}}"#),
            occurred_at_ms: 100,
        }],
        effects: vec![EffectIntentAppend {
            token: TOKEN.into(),
            task_id: None,
            attempt_id: None,
            lease_epoch: Some(1),
            correlation_id: "correlation-test".into(),
            effect_kind: "worker.spawn".into(),
            request_json: r#"{"worker":"fake"}"#.into(),
            created_at_ms: 100,
        }],
        outcome_json: r#"{"effect":"pending"}"#.into(),
        committed_at_ms: 100,
        expires_at_ms: 86_400_100,
    }
}

async fn seed(database: &str) {
    let store = Store::open(database).await.expect("open control database");
    assert!(matches!(
        store
            .persist(&intent_batch())
            .await
            .expect("persist intent"),
        CommitResult::Applied { .. }
    ));
}

fn crash_helper(args: &[&str]) {
    let status = Command::new(env!("CARGO_BIN_EXE_effect-crash-helper"))
        .args(args)
        .status()
        .expect("run crash helper");
    assert!(!status.success(), "helper must be terminated at failpoint");
}

async fn settle(database: &str, inventory: &str, now_ms: i64) -> ReconcileDirective {
    let store = Store::open(database)
        .await
        .expect("reopen control database");
    let external = FakeInventory::open(inventory)
        .await
        .expect("reopen external inventory");
    FakeEffectRunner::new(store, external, "fake-runner", 50)
        .reconcile(TOKEN, now_ms, CrashPoint::Never)
        .await
        .expect("reconcile after crash")
}

#[tokio::test]
async fn abort_before_intent_commit_leaves_no_partial_state() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let (database, _inventory) = paths(&directory);
    drop(
        Store::open(&database)
            .await
            .expect("migrate control database"),
    );

    crash_helper(&["before-intent-commit", &database, TOKEN]);

    let reopened = Store::open(&database).await.expect("reopen after abort");
    assert_eq!(reopened.effect(TOKEN).await.expect("read effect"), None);
}

#[tokio::test]
async fn every_committed_effect_edge_converges_after_process_abort() {
    for (point, resume_at, expected_created_at) in [
        ("before_claim", 200, 200),
        ("after_claim", 251, 251),
        ("after_external_effect", 251, 200),
        ("after_observation", 201, 200),
        ("after_done", 201, 200),
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let (database, inventory) = paths(&directory);
        seed(&database).await;

        crash_helper(&["reconcile", &database, &inventory, TOKEN, "200", point]);

        let directive = settle(&database, &inventory, resume_at).await;
        let resource = match directive {
            ReconcileDirective::Settled(resource)
            | ReconcileDirective::AlreadySettled(resource) => resource,
            other => panic!("unexpected directive after {point}: {other:?}"),
        };
        assert_eq!(resource.created_at_ms, expected_created_at, "point {point}");

        let store = Store::open(&database).await.expect("final control reopen");
        assert_eq!(
            store
                .effect(TOKEN)
                .await
                .expect("final effect")
                .unwrap()
                .status,
            EffectStatus::Done,
            "point {point}"
        );
        let replay = settle(&database, &inventory, resume_at + 1).await;
        assert!(matches!(replay, ReconcileDirective::AlreadySettled(_)));
    }
}

#[tokio::test]
async fn contradictory_and_unmatched_inventory_fail_closed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let (database, inventory_path) = paths(&directory);
    seed(&database).await;
    let inventory = FakeInventory::open(&inventory_path)
        .await
        .expect("open inventory");
    inventory
        .inject(&FakeResource {
            token: TOKEN.into(),
            effect_kind: "different.effect".into(),
            request_json: "{}".into(),
            resource_id: "fake:contradiction".into(),
            created_at_ms: 150,
        })
        .await
        .expect("inject contradiction");
    let store = Store::open(&database).await.expect("open control");
    let runner = FakeEffectRunner::new(store.clone(), inventory, "fake-runner", 50);
    let directive = runner
        .reconcile(TOKEN, 200, CrashPoint::Never)
        .await
        .expect("contradiction becomes directive");
    assert!(matches!(directive, ReconcileDirective::BlockTask { .. }));
    assert_eq!(
        store.effect(TOKEN).await.expect("effect").unwrap().status,
        EffectStatus::Abandoned
    );

    let unmatched_inventory_path = directory.path().join("unmatched.sqlite3");
    let unmatched = FakeInventory::open(&unmatched_inventory_path)
        .await
        .expect("open unmatched inventory");
    unmatched
        .inject(&FakeResource {
            token: "orphan-token".into(),
            effect_kind: "worker.spawn".into(),
            request_json: "{}".into(),
            resource_id: "fake:orphan".into(),
            created_at_ms: 300,
        })
        .await
        .expect("inject orphan");
    let runner = FakeEffectRunner::new(store, unmatched, "fake-runner", 50);
    assert!(matches!(
        runner
            .reconcile("orphan-token", 301, CrashPoint::Never)
            .await
            .expect("orphan becomes directive"),
        ReconcileDirective::BlockTask { task_id: None, .. }
    ));
}
