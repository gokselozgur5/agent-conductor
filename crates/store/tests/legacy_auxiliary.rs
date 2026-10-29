use agent_conductor_store::Store;
use sqlx::{Connection, Executor, SqliteConnection, migrate::Migrator};
use std::path::Path;

async fn legacy(path: &Path, version: u32) -> SqliteConnection {
    let migrations = tempfile::tempdir().unwrap();
    for entry in
        std::fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations")).unwrap()
    {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name = name.to_str().unwrap();
        if name.split('_').next().unwrap().parse::<u32>().unwrap() <= version {
            std::fs::copy(entry.path(), migrations.path().join(name)).unwrap();
        }
    }
    let mut c = SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
        .await
        .unwrap();
    Migrator::new(migrations.path())
        .await
        .unwrap()
        .run(&mut c)
        .await
        .unwrap();
    c.execute("INSERT INTO projects VALUES ('p','/project','device:inode',NULL,0); UPDATE runtime_metadata SET boot_generation=3;").await.unwrap();
    c
}
async fn task(c: &mut SqliteConnection, id: &str, state: &str, history: bool) {
    let aggregate = serde_json::json!({"id":id,"state":state,"version":1}).to_string();
    sqlx::query("INSERT INTO tasks(id,project_id,objective,state,priority,version,authority_json,created_at_ms,updated_at_ms,aggregate_json) VALUES(?,'p','fixture',?,0,1,'{}',0,0,?)").bind(id).bind(state).bind(aggregate).execute(&mut *c).await.unwrap();
    if history {
        sqlx::query("INSERT INTO task_transitions(id,task_id,actor_id,prior_state,new_state,reason,correlation_id,occurred_at_ms) VALUES(?,?,'fixture','running','verifying','fixture','fixture',0)").bind(id).bind(id).execute(&mut *c).await.unwrap();
    }
}

#[tokio::test]
async fn schema19_uncertainty_survives_upgrade_without_fabricated_processes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let mut c = legacy(&path, 19).await;
    for (id, state, history) in [
        ("verifying", "verifying", false),
        ("paused", "paused", true),
        ("cancelled", "cancelled", true),
        ("cleared", "ready", true),
        ("completed", "completed", true),
        ("fresh", "ready", false),
    ] {
        task(&mut c, id, state, history).await;
    }
    c.execute("INSERT INTO workflow_dispatch(task_id,plan_json,status,created_at_ms,updated_at_ms) VALUES ('cleared','{}','queued',0,0),('fresh','{}','queued',1,1)").await.unwrap();
    let before: Vec<(String, String)> =
        sqlx::query_as("SELECT id,aggregate_json FROM tasks ORDER BY id")
            .fetch_all(&mut c)
            .await
            .unwrap();
    let store = Store::open(&path).await.unwrap();
    let after: Vec<(String, String)> =
        sqlx::query_as("SELECT id,aggregate_json FROM tasks ORDER BY id")
            .fetch_all(store.readers())
            .await
            .unwrap();
    assert_eq!(before, after);
    for id in ["verifying", "paused", "cancelled", "cleared"] {
        assert!(store.has_pending_auxiliary(id).await.unwrap());
    }
    for id in ["completed", "fresh"] {
        assert!(!store.has_pending_auxiliary(id).await.unwrap());
    }
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM auxiliary_invocations")
        .fetch_one(store.readers())
        .await
        .unwrap();
    assert_eq!(n, 0);
    assert!(
        store
            .check_runtime_invariants()
            .await
            .unwrap()
            .iter()
            .any(|v| v.code == "legacy_auxiliary_ownership_unknown"
                && v.context_json.contains("verification_without_invocation"))
    );
    assert!(
        sqlx::query("UPDATE tasks SET state='claimed' WHERE id='cleared'")
            .execute(&mut c)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE workflow_dispatch SET status='running' WHERE task_id='cleared'")
            .execute(&mut c)
            .await
            .is_err()
    );
    // A blocked legacy queue entry must not stop an unrelated eligible workflow.
    let claimed = store.claim_workflow(3, 100).await.unwrap().unwrap();
    assert_eq!(claimed.task_id, "fresh");
    c.execute("UPDATE workflow_dispatch SET status='needs_recovery' WHERE task_id='cleared'")
        .await
        .unwrap();
    for result in [
        store.requeue_workflow("cleared", 1, 100).await,
        store
            .auto_requeue_workflow("cleared", 1, 3, 100)
            .await
            .map(|_| ()),
        store
            .reassign_workflow("cleared", 1, "worker", "{}", "attempt", 100)
            .await,
    ] {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("legacy auxiliary ownership unresolved")
        );
    }
    for sql in [
        "INSERT INTO executions(effect_token,task_id,attempt_id,lease_epoch,runner_id,packet_json,packet_sha256,state,created_at_ms,updated_at_ms) VALUES('new','verifying','no-attempt',1,'fixture','{}','hash','prepared',0,0)",
        "INSERT INTO auxiliary_invocations(invocation_id,task_id,execution_token,preparation_json,boot_generation,state,created_at_ms) VALUES('new','verifying','no-token','{}',3,'pending',0)",
        "INSERT INTO workflow_dispatch(task_id,plan_json,status,created_at_ms,updated_at_ms) VALUES('verifying','{}','queued',0,0)",
    ] {
        assert!(
            c.execute(sql)
                .await
                .unwrap_err()
                .to_string()
                .contains("legacy auxiliary ownership unresolved")
        );
    }
    let preserved: (String, i64) = sqlx::query_as(
        "SELECT status,auto_recovery_count FROM workflow_dispatch WHERE task_id='cleared'",
    )
    .fetch_one(store.readers())
    .await
    .unwrap();
    assert_eq!(preserved, ("needs_recovery".into(), 0));
    drop(store);
    let reopened = Store::open(&path).await.unwrap();
    assert!(reopened.has_pending_auxiliary("cleared").await.unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM legacy_auxiliary_ownership")
            .fetch_one(reopened.readers())
            .await
            .unwrap(),
        4
    );
}

#[tokio::test]
async fn pending_upgrade_distinguishes_legacy_and_bound_invocations() {
    for (version, bound) in [(20, false), (21, false), (21, true)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite3");
        let mut c = legacy(&path, version).await;
        task(&mut c, "task", "verifying", true).await;
        c.execute("INSERT INTO agents VALUES ('worker','fake','v1','hash','read_write',0);
        INSERT INTO attempts (id,task_id,worker_id,lease_epoch,instruction_hashes_json,required_checks_json,capability_hash,started_at_ms) VALUES ('attempt','task','worker',1,'[]','[]','hash',0);
        INSERT INTO effect_intents (token,task_id,attempt_id,lease_epoch,correlation_id,effect_kind,request_json,status,created_at_ms,updated_at_ms) VALUES ('fixture','task','attempt',1,'fixture','worker.spawn','{}','pending',0,0);
        INSERT INTO executions (effect_token,task_id,attempt_id,lease_epoch,runner_id,packet_json,packet_sha256,state,created_at_ms,updated_at_ms) VALUES ('fixture','task','attempt',1,'fixture','{}','hash','fenced',0,0);").await.unwrap();
        let preparation=serde_json::json!({"invocation_id":uuid::Uuid::now_v7().to_string(),"task_id":"task","execution_token":"fixture","attempt_id":"attempt","task_version":1,"boot_generation":3,"kind":"check","source":format!("snapshot:{}","a".repeat(64)),"definition_sha256":"b".repeat(64),"workspace":"/private/check"}).to_string();
        sqlx::query("INSERT INTO auxiliary_invocations(invocation_id,task_id,execution_token,preparation_json,boot_generation,state,created_at_ms) VALUES(json_extract(?1,'$.invocation_id'),'task','fixture',?1,3,'pending',0)").bind(&preparation).execute(&mut c).await.unwrap();
        if bound {
            let binding=serde_json::json!({"launch_sha256":"a".repeat(64),"authority_id":"b".repeat(64),"receipt_root":"/private/receipts","wrapper_path":"/private/runner","wrapper_sha256":"c".repeat(64)}).to_string();
            sqlx::query("UPDATE auxiliary_invocations SET launch_binding_json=?")
                .bind(binding)
                .execute(&mut c)
                .await
                .unwrap();
        }
        let store = Store::open(&path).await.unwrap();
        let reason: Option<String> = sqlx::query_scalar(
            "SELECT reason FROM legacy_auxiliary_ownership WHERE task_id='task'",
        )
        .fetch_optional(store.readers())
        .await
        .unwrap();
        assert_eq!(
            reason,
            if bound {
                None
            } else {
                Some("unbound_pending_invocation".into())
            }
        );
        assert_eq!(
            store.pending_auxiliary_invocations().await.unwrap().len(),
            usize::from(bound)
        );
        let row: (String, Option<String>, i64) =
            sqlx::query_as("SELECT state,process_json,release_recorded FROM auxiliary_invocations")
                .fetch_one(store.readers())
                .await
                .unwrap();
        assert_eq!(row, ("pending".into(), None, 0));
        assert!(store.has_pending_auxiliary("task").await.unwrap());
        if !bound {
            prove_unbound_host_resolution(&store, &mut c, &preparation).await;
        }
    }
}

fn host(generation: i64, boot: char) -> agent_conductor_store::AuxiliaryHostObservation {
    agent_conductor_store::AuxiliaryHostObservation {
        authority_id: "a".repeat(64),
        generation,
        platform: "macos".into(),
        host_id: "b".repeat(64),
        boot_id: boot.to_string().repeat(64),
    }
}
async fn reserve(store: &Store, generation: u64) {
    store
        .start_reserved_runtime(100, &"a".repeat(64), generation)
        .await
        .unwrap();
}

#[tokio::test]
async fn legacy_host_requires_first_witness_then_same_host_later_boot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let mut c = legacy(&path, 19).await;
    for state in ["verifying", "paused", "cancelled"] {
        task(&mut c, state, state, true).await;
    }
    let before: Vec<(String, String)> =
        sqlx::query_as("SELECT id,aggregate_json FROM tasks ORDER BY id")
            .fetch_all(&mut c)
            .await
            .unwrap();
    let store = Store::open(&path).await.unwrap();
    reserve(&store, 4).await;
    assert_eq!(
        store
            .reconcile_legacy_auxiliary_host(&host(4, 'c'), 0)
            .await
            .unwrap(),
        0
    );
    assert!(!store.needs_legacy_host_observation(4).await.unwrap());
    assert!(store.has_pending_auxiliary("verifying").await.unwrap());
    reserve(&store, 5).await;
    assert_eq!(
        store
            .reconcile_legacy_auxiliary_host(&host(5, 'c'), 0)
            .await
            .unwrap(),
        0
    );
    assert!(store.has_pending_auxiliary("verifying").await.unwrap());
    assert!(
        store
            .reconcile_legacy_auxiliary_host(&host(4, 'd'), 0)
            .await
            .is_err()
    );
    reserve(&store, 6).await;
    assert_eq!(
        store
            .reconcile_legacy_auxiliary_host(&host(6, 'd'), 0)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        store
            .reconcile_legacy_auxiliary_host(&host(6, 'd'), 0)
            .await
            .unwrap(),
        0
    );
    assert!(!store.has_pending_auxiliary("verifying").await.unwrap());
    let after: Vec<(String, String)> =
        sqlx::query_as("SELECT id,aggregate_json FROM tasks ORDER BY id")
            .fetch_all(&mut c)
            .await
            .unwrap();
    assert_eq!(before, after);
    let audit: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_events WHERE kind='auxiliary.legacy_host_settled'",
    )
    .fetch_one(&mut c)
    .await
    .unwrap();
    assert_eq!(audit, 3);
    let fake: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM auxiliary_invocations")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(fake, 0);
    drop(store);
    let reopened = Store::open(&path).await.unwrap();
    assert!(!reopened.has_pending_auxiliary("paused").await.unwrap());
}

#[tokio::test]
async fn legacy_host_rejects_foreign_and_changed_evidence() {
    for alteration in ["host", "authority", "boot_shape", "marker", "ownership"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite3");
        let mut c = legacy(&path, 19).await;
        task(&mut c, "task", "verifying", true).await;
        let store = Store::open(&path).await.unwrap();
        reserve(&store, 4).await;
        store
            .reconcile_legacy_auxiliary_host(&host(4, 'c'), 0)
            .await
            .unwrap();
        reserve(&store, 5).await;
        let mut next = host(5, 'd');
        match alteration {
            "host" => next.host_id = "e".repeat(64),
            "authority" => next.authority_id = "e".repeat(64),
            "boot_shape" => next.boot_id = "bad".into(),
            "marker" => {
                c.execute("UPDATE legacy_auxiliary_ownership SET recorded_boot_generation=4")
                    .await
                    .unwrap();
            }
            "ownership" => {
                c.execute("UPDATE legacy_auxiliary_host_witness SET ownership_json='{}'")
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let result = store.reconcile_legacy_auxiliary_host(&next, 0).await;
        assert!(result.is_err() || result.unwrap() == 0);
        assert!(store.has_pending_auxiliary("task").await.unwrap());
    }
}

#[tokio::test]
async fn legacy_host_audit_failure_rolls_back_settlement_and_witness() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let mut c = legacy(&path, 19).await;
    task(&mut c, "task", "verifying", true).await;
    let store = Store::open(&path).await.unwrap();
    reserve(&store, 4).await;
    c.execute("CREATE TRIGGER reject_host_audit BEFORE INSERT ON audit_events WHEN NEW.kind LIKE 'auxiliary.legacy_host_%' BEGIN SELECT RAISE(ABORT,'injected'); END").await.unwrap();
    assert!(
        store
            .reconcile_legacy_auxiliary_host(&host(4, 'c'), 0)
            .await
            .is_err()
    );
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM legacy_auxiliary_host_witness")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(n, 0);
    c.execute("DROP TRIGGER reject_host_audit").await.unwrap();
    store
        .reconcile_legacy_auxiliary_host(&host(4, 'c'), 0)
        .await
        .unwrap();
    reserve(&store, 5).await;
    c.execute("CREATE TRIGGER reject_host_audit BEFORE INSERT ON audit_events WHEN NEW.kind='auxiliary.legacy_host_settled' BEGIN SELECT RAISE(ABORT,'injected'); END").await.unwrap();
    assert!(
        store
            .reconcile_legacy_auxiliary_host(&host(5, 'd'), 0)
            .await
            .is_err()
    );
    let row: (String, i64) =
        sqlx::query_as("SELECT status,checked_generation FROM legacy_auxiliary_host_witness")
            .fetch_one(&mut c)
            .await
            .unwrap();
    assert_eq!(row, ("observed".into(), 4));
    assert!(store.has_pending_auxiliary("task").await.unwrap());
    c.execute("DROP TRIGGER reject_host_audit").await.unwrap();
    assert_eq!(
        store
            .reconcile_legacy_auxiliary_host(&host(5, 'd'), 0)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn legacy_host_batches_do_not_starve_unobserved_or_unsettled_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let mut c = legacy(&path, 19).await;
    for i in 0..130 {
        task(&mut c, &format!("task-{i:03}"), "paused", true).await;
    }
    let store = Store::open(&path).await.unwrap();
    reserve(&store, 4).await;
    assert_eq!(
        store
            .reconcile_legacy_auxiliary_host(&host(4, 'c'), 0)
            .await
            .unwrap(),
        0
    );
    assert!(store.needs_legacy_host_observation(4).await.unwrap());
    assert_eq!(
        store
            .reconcile_legacy_auxiliary_host(&host(4, 'c'), 0)
            .await
            .unwrap(),
        0
    );
    assert!(!store.needs_legacy_host_observation(4).await.unwrap());
    reserve(&store, 5).await;
    assert_eq!(
        store
            .reconcile_legacy_auxiliary_host(&host(5, 'd'), 0)
            .await
            .unwrap(),
        128
    );
    assert_eq!(
        store
            .reconcile_legacy_auxiliary_host(&host(5, 'd'), 0)
            .await
            .unwrap(),
        2
    );
    assert!(!store.needs_legacy_host_observation(5).await.unwrap());
}

async fn prove_unbound_host_resolution(store: &Store, c: &mut SqliteConnection, preparation: &str) {
    reserve(store, 4).await;
    store
        .reconcile_legacy_auxiliary_host(&host(4, 'c'), 0)
        .await
        .unwrap();
    assert_unbound_contradictions(c).await;
    reserve(store, 5).await;
    // Fault after all invocation updates: no partial settlement may escape.
    c.execute("CREATE TRIGGER fail_legacy_delete BEFORE DELETE ON legacy_auxiliary_ownership BEGIN SELECT RAISE(ABORT,'injected'); END").await.unwrap();
    assert!(
        store
            .reconcile_legacy_auxiliary_host(&host(5, 'd'), 10)
            .await
            .is_err()
    );
    let state: String = sqlx::query_scalar("SELECT state FROM auxiliary_invocations")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    assert_eq!(state, "pending");
    c.execute("DROP TRIGGER fail_legacy_delete").await.unwrap();
    assert_eq!(
        store
            .reconcile_legacy_auxiliary_host(&host(5, 'd'), 10)
            .await
            .unwrap(),
        1
    );
    let row:(String,Option<String>,i64,String,String)=sqlx::query_as("SELECT state,process_json,release_recorded,preparation_json,settlement_json FROM auxiliary_invocations").fetch_one(&mut *c).await.unwrap();
    assert_eq!(row.0, "settled");
    assert_eq!(row.1, None);
    assert_eq!(row.2, 0);
    assert_eq!(row.3, preparation);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&row.4).unwrap()["observation"],
        "native_host_boot_transition"
    );
    assert!(!store.has_pending_auxiliary("task").await.unwrap());
    let publications: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM auxiliary_publications")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    assert_eq!(publications, 0);
}

async fn assert_unbound_contradictions(c: &mut SqliteConnection) {
    for sql in [
        "UPDATE auxiliary_invocations SET release_recorded=1",
        "UPDATE auxiliary_invocations SET process_json='{}'",
        "UPDATE auxiliary_invocations SET launch_binding_json='{}'",
        "UPDATE auxiliary_invocations SET preparation_json=json_set(preparation_json,'$.workspace','/changed')",
        "UPDATE auxiliary_invocations SET preparation_json=json_set(preparation_json,'$.boot_generation',6),boot_generation=6",
    ] {
        // Each corruption has its own copy; never reset a refusal in the product.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("negative.sqlite3");
        sqlx::query("VACUUM main INTO ?")
            .bind(path.to_str().unwrap())
            .execute(&mut *c)
            .await
            .unwrap();
        let store = Store::open(&path).await.unwrap();
        reserve(&store, 5).await;
        let mut edit = SqliteConnection::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        edit.execute(sql).await.unwrap();
        assert_eq!(
            store
                .reconcile_legacy_auxiliary_host(&host(5, 'd'), 0)
                .await
                .unwrap(),
            0
        );
        assert!(store.has_pending_auxiliary("task").await.unwrap());
        let state: String = sqlx::query_scalar("SELECT status FROM legacy_auxiliary_host_witness")
            .fetch_one(&mut edit)
            .await
            .unwrap();
        assert_eq!(state, "contradiction");
        reserve(&store, 6).await;
        assert_eq!(
            store
                .reconcile_legacy_auxiliary_host(&host(6, 'e'), 0)
                .await
                .unwrap(),
            0
        );
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_events WHERE kind='auxiliary.legacy_host_settled'",
        )
        .fetch_one(&mut edit)
        .await
        .unwrap();
        assert_eq!(n, 0);
    }
}
