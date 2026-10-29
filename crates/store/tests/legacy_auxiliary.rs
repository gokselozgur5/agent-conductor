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
    }
}
