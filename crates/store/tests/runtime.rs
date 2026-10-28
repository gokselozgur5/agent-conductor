use agent_conductor_store::Store;
use sqlx::{Connection, Row, SqliteConnection};

#[tokio::test]
async fn reopening_the_database_advances_the_boot_generation() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database = directory.path().join("conductor.sqlite3");

    let first = Store::open(&database).await.expect("first store opens");
    let first_boot = first.start_runtime(100).await.expect("first boot starts");
    assert_eq!(first_boot.boot_generation, 1);
    assert!(
        first
            .check_runtime_invariants()
            .await
            .expect("first invariant scan")
            .is_empty()
    );
    drop(first);

    let second = Store::open(&database).await.expect("store reopens");
    let second_boot = second.start_runtime(200).await.expect("second boot starts");
    assert_eq!(second_boot.boot_generation, 2);
    assert_eq!(second_boot.started_at_ms, 200);
    assert!(
        second
            .check_runtime_invariants()
            .await
            .expect("second invariant scan")
            .is_empty()
    );
}

#[tokio::test]
async fn invariant_scan_is_degraded_until_runtime_startup_commits() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database = directory.path().join("conductor.sqlite3");
    let store = Store::open(database).await.expect("store opens");

    let violations = store
        .check_runtime_invariants()
        .await
        .expect("invariant scan");
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].code, "runtime_generation");
}

#[tokio::test]
async fn restart_fences_live_leases_into_a_bounded_recovery_window() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database = directory.path().join("conductor.sqlite3");
    let store = Store::open(&database).await.expect("store opens");
    let url = format!("sqlite://{}", database.display());
    let mut seed = SqliteConnection::connect(&url)
        .await
        .expect("seed connection opens");
    sqlx::query("INSERT INTO projects (id, canonical_path, filesystem_identity, created_at_ms) VALUES ('project-1', '/project', 'dev:1:ino:1', 1)")
        .execute(&mut seed).await.expect("project seeds");
    sqlx::query("INSERT INTO tasks (id, project_id, objective, state, priority, version, authority_json, created_at_ms, updated_at_ms) VALUES ('task-1', 'project-1', 'recover', 'running', 0, 2, '{}', 1, 2)")
        .execute(&mut seed).await.expect("task seeds");
    sqlx::query("INSERT INTO agents (id, adapter_kind, protocol_fingerprint, capability_hash, access_mode, created_at_ms) VALUES ('agent-1', 'fake', 'v1', 'hash', 'read_write', 1)")
        .execute(&mut seed).await.expect("agent seeds");
    sqlx::query("INSERT INTO attempts (id, task_id, worker_id, lease_epoch, instruction_hashes_json, required_checks_json, capability_hash, started_at_ms) VALUES ('attempt-1', 'task-1', 'agent-1', 4, '[]', '[]', 'hash', 2)")
        .execute(&mut seed).await.expect("attempt seeds");
    sqlx::query("INSERT INTO leases (attempt_id, task_id, epoch, status, boot_generation, remaining_ms, grace_ms, display_expires_at_ms, last_heartbeat_at_ms) VALUES ('attempt-1', 'task-1', 4, 'active', 7, 9000, 3000, 10000, 1000)")
        .execute(&mut seed).await.expect("lease seeds");
    drop(seed);

    let startup = store.start_runtime(20_000).await.expect("restart begins");
    assert_eq!(startup.boot_generation, 1);
    assert_eq!(startup.recovery_pending.len(), 1);
    assert_eq!(startup.recovery_pending[0].task_id, "task-1");
    assert_eq!(startup.recovery_pending[0].attempt_id, "attempt-1");
    assert_eq!(startup.recovery_pending[0].epoch, 4);
    assert_eq!(startup.recovery_pending[0].recovery_window_ms, 3000);

    let lease = sqlx::query(
        "SELECT status, boot_generation, remaining_ms, display_expires_at_ms FROM leases WHERE attempt_id = 'attempt-1'",
    )
    .fetch_one(store.readers())
    .await
    .expect("recovered lease reads");
    assert_eq!(lease.get::<String, _>("status"), "recovery_pending");
    assert_eq!(lease.get::<i64, _>("boot_generation"), 1);
    assert_eq!(lease.get::<i64, _>("remaining_ms"), 3000);
    assert_eq!(lease.get::<i64, _>("display_expires_at_ms"), 23_000);
}
