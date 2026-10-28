use agent_conductor_store::{
    AuditAppend, CommitBatch, CommitResult, EffectIntentAppend, NewProject, NewTask, Store,
    StoreError, TaskMutation, TaskTransitionAppend,
};
use sqlx::{Connection, Row, SqliteConnection};
use tempfile::TempDir;

async fn store() -> (TempDir, Store) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database = directory.path().join("conductor.sqlite3");
    let store = Store::open(database).await.expect("open store");
    (directory, store)
}

async fn seeded_store() -> (TempDir, Store) {
    let (directory, store) = store().await;
    store
        .create_project(NewProject {
            id: "project-1",
            canonical_path: "/workspace/project",
            filesystem_identity: "dev:1:ino:2",
            git_common_dir_identity: Some("git:project-1"),
            created_at_ms: 100,
        })
        .await
        .expect("create project");
    store
        .create_task(NewTask {
            id: "task-1",
            project_id: "project-1",
            objective: "Prove atomic persistence",
            state: "ready",
            priority: 10,
            authority_json: "{}",
            created_at_ms: 101,
        })
        .await
        .expect("create task");
    (directory, store)
}

fn batch(key: &str, hash: &str, audit_id: &str, effect_token: &str) -> CommitBatch {
    CommitBatch {
        actor_id: "operator-1".into(),
        idempotency_key: key.into(),
        request_hash: hash.into(),
        task: Some(TaskMutation {
            task_id: "task-1".into(),
            expected_version: 0,
            new_version: 1,
            state: "claimed".into(),
            updated_at_ms: 200,
        }),
        transitions: vec![TaskTransitionAppend {
            id: format!("transition-{key}"),
            task_id: "task-1".into(),
            actor_id: "operator-1".into(),
            prior_state: "ready".into(),
            new_state: "claimed".into(),
            reason: "accepted claim".into(),
            correlation_id: "correlation-1".into(),
            occurred_at_ms: 200,
        }],
        audit: vec![AuditAppend {
            id: audit_id.into(),
            task_id: Some("task-1".into()),
            actor_id: "operator-1".into(),
            kind: "task.claimed".into(),
            correlation_id: "correlation-1".into(),
            payload_json: "{}".into(),
            occurred_at_ms: 200,
        }],
        effects: vec![EffectIntentAppend {
            token: effect_token.into(),
            task_id: Some("task-1".into()),
            attempt_id: None,
            lease_epoch: Some(1),
            correlation_id: "correlation-1".into(),
            effect_kind: "worker.spawn".into(),
            request_json: "{}".into(),
            created_at_ms: 200,
        }],
        outcome_json: r#"{"state":"claimed","version":1}"#.into(),
        committed_at_ms: 200,
        expires_at_ms: 86_400_200,
    }
}

#[tokio::test]
async fn migrations_enable_required_sqlite_settings() {
    let (_directory, store) = store().await;
    let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(store.readers())
        .await
        .expect("journal mode");
    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(store.readers())
        .await
        .expect("foreign keys");
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('tasks', 'attempts', 'leases', 'audit_events', 'effect_intents', 'command_idempotency')",
    )
    .fetch_one(store.readers())
    .await
    .expect("schema tables");

    assert_eq!(journal_mode, "wal");
    assert_eq!(foreign_keys, 1);
    assert_eq!(tables, 6);
}

#[tokio::test]
async fn reader_pool_cannot_mutate_authoritative_state() {
    let (_directory, store) = seeded_store().await;
    let error = sqlx::query("UPDATE tasks SET state = 'cancelled' WHERE id = 'task-1'")
        .execute(store.readers())
        .await
        .expect_err("reader write must fail");

    assert!(error.to_string().contains("readonly"));
}

#[tokio::test]
async fn structural_index_rejects_two_open_attempts_for_one_task() {
    let (directory, store) = seeded_store().await;
    let url = format!(
        "sqlite://{}",
        directory.path().join("conductor.sqlite3").display()
    );
    let mut connection = SqliteConnection::connect(&url)
        .await
        .expect("test-only writer connection");
    sqlx::query(
        "INSERT INTO agents (id, adapter_kind, protocol_fingerprint, capability_hash, access_mode, created_at_ms) VALUES ('agent-1', 'fake', 'v1', 'hash', 'read_write', 100)",
    )
    .execute(&mut connection)
    .await
    .expect("agent");
    sqlx::query(
        "INSERT INTO attempts (id, task_id, worker_id, lease_epoch, instruction_hashes_json, required_checks_json, capability_hash, started_at_ms) VALUES ('attempt-1', 'task-1', 'agent-1', 1, '[]', '[]', 'hash', 101)",
    )
    .execute(&mut connection)
    .await
    .expect("first attempt");

    let error = sqlx::query(
        "INSERT INTO attempts (id, task_id, worker_id, lease_epoch, instruction_hashes_json, required_checks_json, capability_hash, started_at_ms) VALUES ('attempt-2', 'task-1', 'agent-1', 2, '[]', '[]', 'hash', 102)",
    )
    .execute(&mut connection)
    .await
    .expect_err("second open attempt must fail");

    assert!(error.to_string().contains("UNIQUE constraint failed"));
    drop(store);
}

#[tokio::test]
async fn outcome_audit_effect_and_idempotency_commit_atomically() {
    let (_directory, store) = seeded_store().await;
    let batch = batch("key-1", "hash-1", "audit-1", "effect-1");

    let result = store.persist(&batch).await.expect("persist outcome");
    assert_eq!(
        result,
        CommitResult::Applied {
            outcome_json: batch.outcome_json.clone()
        }
    );

    let task = sqlx::query("SELECT state, version FROM tasks WHERE id = 'task-1'")
        .fetch_one(store.readers())
        .await
        .expect("task");
    assert_eq!(task.get::<String, _>("state"), "claimed");
    assert_eq!(task.get::<i64, _>("version"), 1);
    assert_eq!(count(store.readers(), "audit_events").await, 1);
    assert_eq!(count(store.readers(), "task_transitions").await, 1);
    assert_eq!(count(store.readers(), "effect_intents").await, 1);
    assert_eq!(count(store.readers(), "command_idempotency").await, 1);
}

#[tokio::test]
async fn identical_command_replays_stored_outcome_without_duplicate_rows() {
    let (_directory, store) = seeded_store().await;
    let batch = batch("key-1", "hash-1", "audit-1", "effect-1");
    store.persist(&batch).await.expect("first persistence");

    let replay = store.persist(&batch).await.expect("idempotent replay");
    assert_eq!(
        replay,
        CommitResult::Replayed {
            outcome_json: batch.outcome_json
        }
    );
    assert_eq!(count(store.readers(), "audit_events").await, 1);
    assert_eq!(count(store.readers(), "task_transitions").await, 1);
    assert_eq!(count(store.readers(), "effect_intents").await, 1);
}

#[tokio::test]
async fn idempotency_key_with_different_request_is_rejected() {
    let (_directory, store) = seeded_store().await;
    store
        .persist(&batch("key-1", "hash-1", "audit-1", "effect-1"))
        .await
        .expect("first persistence");

    let error = store
        .persist(&batch("key-1", "different", "audit-2", "effect-2"))
        .await
        .expect_err("collision must fail");
    assert!(matches!(error, StoreError::IdempotencyCollision));
    assert_eq!(count(store.readers(), "audit_events").await, 1);
    assert_eq!(count(store.readers(), "task_transitions").await, 1);
    assert_eq!(count(store.readers(), "effect_intents").await, 1);
}

#[tokio::test]
async fn optimistic_conflict_rolls_back_every_append() {
    let (_directory, store) = seeded_store().await;
    let mut conflicting = batch("key-1", "hash-1", "audit-1", "effect-1");
    conflicting
        .task
        .as_mut()
        .expect("task mutation")
        .expected_version = 7;
    conflicting
        .task
        .as_mut()
        .expect("task mutation")
        .new_version = 8;

    let error = store
        .persist(&conflicting)
        .await
        .expect_err("version conflict");
    assert!(matches!(error, StoreError::VersionConflict { .. }));
    assert_eq!(count(store.readers(), "audit_events").await, 0);
    assert_eq!(count(store.readers(), "task_transitions").await, 0);
    assert_eq!(count(store.readers(), "effect_intents").await, 0);
    assert_eq!(count(store.readers(), "command_idempotency").await, 0);
}

#[tokio::test]
async fn invalid_version_step_rolls_back_every_append() {
    let (_directory, store) = seeded_store().await;
    let mut invalid = batch("key-1", "hash-1", "audit-1", "effect-1");
    invalid.task.as_mut().expect("task mutation").new_version = 3;

    let error = store
        .persist(&invalid)
        .await
        .expect_err("version must advance exactly once");
    assert!(matches!(error, StoreError::InvalidVersionStep { .. }));
    assert_eq!(count(store.readers(), "audit_events").await, 0);
    assert_eq!(count(store.readers(), "task_transitions").await, 0);
    assert_eq!(count(store.readers(), "effect_intents").await, 0);
    assert_eq!(count(store.readers(), "command_idempotency").await, 0);
}

#[tokio::test]
async fn duplicate_transition_rolls_back_the_whole_batch() {
    let (_directory, store) = seeded_store().await;
    store
        .persist(&batch("key-1", "hash-1", "audit-1", "effect-1"))
        .await
        .expect("first persistence");

    let mut duplicate = batch("key-2", "hash-2", "audit-2", "effect-2");
    let task = duplicate.task.as_mut().expect("task mutation");
    task.expected_version = 1;
    task.new_version = 2;
    duplicate.transitions[0].id = "transition-key-1".into();

    duplicate
        .transitions
        .first_mut()
        .expect("transition")
        .prior_state = "claimed".into();
    let error = store
        .persist(&duplicate)
        .await
        .expect_err("duplicate transition must fail");
    assert!(matches!(error, StoreError::Database(_)));

    let version: i64 = sqlx::query_scalar("SELECT version FROM tasks WHERE id = 'task-1'")
        .fetch_one(store.readers())
        .await
        .expect("task version");
    assert_eq!(version, 1);
    assert_eq!(count(store.readers(), "audit_events").await, 1);
    assert_eq!(count(store.readers(), "task_transitions").await, 1);
    assert_eq!(count(store.readers(), "effect_intents").await, 1);
    assert_eq!(count(store.readers(), "command_idempotency").await, 1);
}

#[tokio::test]
async fn close_and_reopen_preserves_committed_truth_and_idempotency() {
    let (directory, store) = seeded_store().await;
    let database = directory.path().join("conductor.sqlite3");
    let committed = batch("key-1", "hash-1", "audit-1", "effect-1");
    store.persist(&committed).await.expect("persist outcome");
    store.readers().close().await;
    drop(store);

    let reopened = Store::open(&database).await.expect("reopen store");
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(reopened.readers())
        .await
        .expect("integrity check");
    assert_eq!(integrity, "ok");

    let task: (String, i64) =
        sqlx::query_as("SELECT state, version FROM tasks WHERE id = 'task-1'")
            .fetch_one(reopened.readers())
            .await
            .expect("restored task");
    assert_eq!(task, ("claimed".into(), 1));
    assert_eq!(count(reopened.readers(), "task_transitions").await, 1);
    assert_eq!(count(reopened.readers(), "audit_events").await, 1);
    assert_eq!(count(reopened.readers(), "effect_intents").await, 1);

    let replay = reopened
        .persist(&committed)
        .await
        .expect("replay after reopen");
    assert_eq!(
        replay,
        CommitResult::Replayed {
            outcome_json: committed.outcome_json
        }
    );
    assert_eq!(count(reopened.readers(), "task_transitions").await, 1);
    assert_eq!(count(reopened.readers(), "audit_events").await, 1);
    assert_eq!(count(reopened.readers(), "effect_intents").await, 1);
}

#[tokio::test]
async fn concurrent_expected_version_writers_have_one_winner() {
    let (_directory, store) = seeded_store().await;
    let first_store = store.clone();
    let second_store = store.clone();
    let first = tokio::spawn(async move {
        first_store
            .persist(&batch("key-1", "hash-1", "audit-1", "effect-1"))
            .await
    });
    let second = tokio::spawn(async move {
        second_store
            .persist(&batch("key-2", "hash-2", "audit-2", "effect-2"))
            .await
    });

    let results = [
        first.await.expect("first join"),
        second.await.expect("second join"),
    ];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(StoreError::VersionConflict { .. })))
            .count(),
        1
    );
}

async fn count(pool: &sqlx::SqlitePool, table: &str) -> i64 {
    let query = format!("SELECT COUNT(*) FROM {table}");
    sqlx::query_scalar(&query)
        .fetch_one(pool)
        .await
        .expect("count rows")
}
