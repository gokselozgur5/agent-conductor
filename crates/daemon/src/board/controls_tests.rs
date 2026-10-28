use super::*;
use agent_conductor_store::Store;
use conductor_protocol::{Request as ControlRequest, Response as ControlResponse};
use conductor_service::PersistentControlService;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn post(address: SocketAddr, auth: &str, extra: &str, body: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("POST /api/control HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{auth}{extra}\r\n{body}", body.len()).as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}
fn input(action: &str, version: u64, key: &str) -> String {
    serde_json::json!({"task_id":"controlled", "action":action, "expected_version":version, "idempotency_key":key}).to_string()
}
async fn assert_state(service: &PersistentControlService, expected: &str, version: u64) {
    let ControlResponse::Task(task) = service
        .handle(ControlRequest::GetTask {
            task_id: "controlled".into(),
        })
        .await
        .unwrap()
    else {
        panic!("task missing")
    };
    assert_eq!(
        serde_json::to_value(&task.aggregate).unwrap()["state"],
        expected
    );
    assert_eq!(task.aggregate.version, version);
}
#[tokio::test]
async fn controls_preserve_auth_versions_idempotency_and_terminal_state() {
    let directory = tempfile::tempdir().unwrap();
    let service = fixture(directory.path()).await;
    let token = "synthetic-control-test-credential-00000";
    let board = OperatorBoard::bind(
        "127.0.0.1:0".parse().unwrap(),
        token.into(),
        service.clone(),
    )
    .await
    .unwrap();
    let address = board.address();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(board.run_until(async {
        let _ = stopped.await;
    }));
    let auth = format!("Authorization: Bearer {token}\r\n");
    let ready = input("ready", 0, "ready");
    assert!(
        post(address, "", "", &ready)
            .await
            .starts_with("HTTP/1.1 401")
    );
    assert!(
        post(address, &auth, "Origin: http://evil.example\r\n", &ready)
            .await
            .starts_with("HTTP/1.1 403")
    );
    assert!(
        post(address, &auth, "", &input("complete", 0, "bad"))
            .await
            .starts_with("HTTP/1.1 400")
    );
    let mut extra = serde_json::from_str::<serde_json::Value>(&ready).unwrap();
    extra["actor"] = serde_json::json!("system");
    assert!(
        post(address, &auth, "", &extra.to_string())
            .await
            .starts_with("HTTP/1.1 400")
    );
    assert!(
        post(address, &auth, "", &"x".repeat(16_385))
            .await
            .starts_with("HTTP/1.1 413")
    );
    assert_state(&service, "backlog", 0).await;
    assert!(
        post(address, &auth, "", &ready)
            .await
            .starts_with("HTTP/1.1 204")
    );
    assert!(
        post(address, &auth, "", &ready)
            .await
            .starts_with("HTTP/1.1 204")
    );
    assert_state(&service, "ready", 1).await;
    assert!(
        post(address, &auth, "", &input("pause", 0, "stale"))
            .await
            .starts_with("HTTP/1.1 409")
    );
    assert_state(&service, "ready", 1).await;
    for (action, version, state) in [
        ("pause", 1, "paused"),
        ("resume", 2, "ready"),
        ("cancel", 3, "cancelled"),
    ] {
        assert!(
            post(address, &auth, "", &input(action, version, action))
                .await
                .starts_with("HTTP/1.1 204")
        );
        assert_state(&service, state, version + 1).await;
    }
    let refused = post(address, &auth, "", &input("resume", 4, "terminal")).await;
    assert!(!refused.starts_with("HTTP/1.1 204"));
    assert_state(&service, "cancelled", 4).await;
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

pub(super) async fn fixture(path: &std::path::Path) -> PersistentControlService {
    let service = PersistentControlService::start(
        Store::open(path.join("state.sqlite3")).await.unwrap(),
        100,
    )
    .await
    .unwrap();
    service
        .handle(ControlRequest::RegisterProject {
            actor_id: "operator".into(),
            idempotency_key: "project".into(),
            id: "project".into(),
            path: path.to_string_lossy().into_owned(),
        })
        .await
        .unwrap();
    service
        .handle(ControlRequest::CreateTask {
            actor_id: "operator".into(),
            idempotency_key: "task".into(),
            id: "controlled".into(),
            project_id: "project".into(),
            objective: "exercise controls".into(),
            priority: 0,
            authority: serde_json::json!({}),
            required_checks: vec![],
        })
        .await
        .unwrap();
    service
}

#[tokio::test]
async fn history_is_scoped_paginated_private_and_read_only() {
    use sqlx::Connection;
    let directory = tempfile::tempdir().unwrap();
    let service = fixture(directory.path()).await;
    let mut db = sqlx::SqliteConnection::connect(&format!(
        "sqlite:{}",
        directory.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    for n in 0..205 {
        sqlx::query("INSERT INTO audit_events (id, task_id, actor_id, kind, correlation_id, payload_json, occurred_at_ms) VALUES (?, 'controlled', '<script>actor</script>', 'synthetic', 'correlation', '{\"private\":\"DO_NOT_PROJECT_PAYLOAD\"}', ?)")
            .bind(format!("history-{n}")).bind(n % 3).execute(&mut db).await.unwrap();
    }
    sqlx::query("INSERT INTO audit_events (id, task_id, actor_id, kind, correlation_id, payload_json, occurred_at_ms) VALUES ('unrelated', NULL, 'operator', 'UNRELATED', 'other', '{}', 999)").execute(&mut db).await.unwrap();
    let token = "synthetic-history-credential-00000000";
    let board = OperatorBoard::bind(
        "127.0.0.1:0".parse().unwrap(),
        token.into(),
        service.clone(),
    )
    .await
    .unwrap();
    let address = board.address();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(board.run_until(async {
        let _ = stopped.await;
    }));
    let auth = format!("Authorization: Bearer {token}\r\n");
    let path = "/api/history?task_id=controlled";
    assert!(
        super::tests::request(address, &address.to_string(), path, "")
            .await
            .starts_with("HTTP/1.1 401")
    );
    let mut before = None;
    let mut sequences = Vec::new();
    for expected in [100, 100, 6] {
        let page = format!(
            "{path}{}",
            before
                .as_ref()
                .map_or_else(String::new, |value| format!("&before={value}"))
        );
        let raw = super::tests::request(address, &address.to_string(), &page, &auth).await;
        assert!(raw.starts_with("HTTP/1.1 200"));
        assert!(!raw.contains("DO_NOT_PROJECT_PAYLOAD"));
        assert!(!raw.contains("UNRELATED"));
        let data: serde_json::Value =
            serde_json::from_str(raw.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        let entries = data["entries"].as_array().unwrap();
        assert_eq!(entries.len(), expected);
        sequences.extend(
            entries
                .iter()
                .map(|entry| entry["sequence"].as_str().unwrap().parse::<i64>().unwrap()),
        );
        before = data["next_before"].as_str().map(str::to_owned);
        if sequences.len() == 100 {
            // New append must not shift an exclusive older-page cursor.
            sqlx::query("INSERT INTO audit_events (id, task_id, actor_id, kind, correlation_id, payload_json, occurred_at_ms) VALUES ('newer', 'controlled', 'operator', 'newer', 'newer', '{}', 0)").execute(&mut db).await.unwrap();
        }
    }
    assert!(before.is_none());
    assert!(sequences.windows(2).all(|pair| pair[0] > pair[1]));
    for cursor in ["0", "-1", "garbage", "9223372036854775808"] {
        assert!(
            service
                .handle(ControlRequest::TaskHistory {
                    task_id: "controlled".into(),
                    before: Some(cursor.into())
                })
                .await
                .is_err()
        );
    }
    assert!(
        service
            .handle(ControlRequest::TaskHistory {
                task_id: "missing".into(),
                before: None
            })
            .await
            .is_err()
    );
    assert_state(&service, "backlog", 0).await;
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn task_pages_do_not_omit_or_duplicate_rows_when_new_tasks_arrive() {
    use sqlx::Connection;
    let directory = tempfile::tempdir().unwrap();
    let service = fixture(directory.path()).await;
    let mut db = sqlx::SqliteConnection::connect(&format!(
        "sqlite:{}",
        directory.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    let baseline: String =
        sqlx::query_scalar("SELECT aggregate_json FROM tasks WHERE id='controlled'")
            .fetch_one(&mut db)
            .await
            .unwrap();
    for n in 0..205 {
        let id = format!("task-{n:03}");
        let mut aggregate: serde_json::Value = serde_json::from_str(&baseline).unwrap();
        aggregate["id"] = serde_json::json!(id);
        sqlx::query("INSERT INTO tasks(id,project_id,objective,state,authority_json,aggregate_json,created_at_ms,updated_at_ms) VALUES(?,'project','page fixture','backlog','{}',?,0,0)")
            .bind(id).bind(aggregate.to_string()).execute(&mut db).await.unwrap();
    }
    let mut after = None;
    let mut ids = Vec::new();
    for expected in [100, 100, 6] {
        let ControlResponse::TaskPage { tasks, next_after } = service
            .handle(ControlRequest::ListTaskPage {
                project_id: Some("project".into()),
                after,
            })
            .await
            .unwrap()
        else {
            panic!("page missing");
        };
        assert_eq!(tasks.len(), expected);
        ids.extend(tasks.into_iter().map(|task| task.aggregate.id.0));
        after = next_after;
        if ids.len() == 100 {
            service
                .handle(ControlRequest::CreateTask {
                    actor_id: "operator".into(),
                    idempotency_key: "insert-earlier".into(),
                    id: "a-new-earlier-id".into(),
                    project_id: "project".into(),
                    objective: "new task".into(),
                    priority: 999,
                    authority: serde_json::json!({}),
                    required_checks: vec![],
                })
                .await
                .unwrap();
        }
    }
    assert!(after.is_none());
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(ids.len(), 206);
    let ControlResponse::TaskPage { tasks, next_after } = service
        .handle(ControlRequest::ListTaskPage {
            project_id: Some("missing".into()),
            after: None,
        })
        .await
        .unwrap()
    else {
        panic!("page missing");
    };
    assert!(tasks.is_empty() && next_after.is_none());
    let ControlResponse::TaskPage { tasks, .. } = service
        .handle(ControlRequest::ListTaskPage {
            project_id: None,
            after: None,
        })
        .await
        .unwrap()
    else {
        panic!("page missing");
    };
    assert_eq!(tasks[0].aggregate.id.0, "a-new-earlier-id");
    assert_state(&service, "backlog", 0).await;
}

#[tokio::test]
async fn decisions_persist_with_atomic_audit_and_reject_stale_or_duplicate_resolution() {
    use sqlx::Connection;
    let directory = tempfile::tempdir().unwrap();
    let service = fixture(directory.path()).await;
    let token = "synthetic-decision-credential-00000000";
    let board = OperatorBoard::bind(
        "127.0.0.1:0".parse().unwrap(),
        token.into(),
        service.clone(),
    )
    .await
    .unwrap();
    let address = board.address();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(board.run_until(async {
        let _ = stopped.await;
    }));
    let auth = format!("Authorization: Bearer {token}\r\n");
    let mut open = serde_json::json!({"task_id":"controlled","expected_version":0,"idempotency_key":"decision-open","action":"open_decision","decision_id":"format","question":"<script>Which format?</script>"});
    assert!(
        post(address, "", "", &open.to_string())
            .await
            .starts_with("HTTP/1.1 401")
    );
    let mut db = sqlx::SqliteConnection::connect(&format!(
        "sqlite:{}",
        directory.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER reject_decision_audit BEFORE INSERT ON audit_events WHEN NEW.kind='open_decision' BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END").execute(&mut db).await.unwrap();
    let failed = post(address, &auth, "", &open.to_string()).await;
    assert!(!failed.starts_with("HTTP/1.1 204"));
    assert_state(&service, "backlog", 0).await;
    sqlx::query("DROP TRIGGER reject_decision_audit")
        .execute(&mut db)
        .await
        .unwrap();
    for _ in 0..2 {
        assert!(
            post(address, &auth, "", &open.to_string())
                .await
                .starts_with("HTTP/1.1 204")
        );
    }
    assert_state(&service, "backlog", 1).await;
    open["question"] = serde_json::json!("conflicting duplicate");
    assert!(
        !post(address, &auth, "", &open.to_string())
            .await
            .starts_with("HTTP/1.1 204")
    );
    let mut resolve = serde_json::json!({"task_id":"controlled","expected_version":0,"idempotency_key":"decision-resolve","action":"resolve_decision","decision_id":"format","rationale":"Use plain text."});
    assert!(
        post(address, &auth, "", &resolve.to_string())
            .await
            .starts_with("HTTP/1.1 409")
    );
    resolve["expected_version"] = serde_json::json!(1);
    for _ in 0..2 {
        assert!(
            post(address, &auth, "", &resolve.to_string())
                .await
                .starts_with("HTTP/1.1 204")
        );
    }
    resolve["expected_version"] = serde_json::json!(2);
    resolve["idempotency_key"] = serde_json::json!("new-resolution");
    assert!(
        !post(address, &auth, "", &resolve.to_string())
            .await
            .starts_with("HTTP/1.1 204")
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM audit_events WHERE task_id='controlled' AND kind IN ('open_decision','resolve_decision')").fetch_one(&mut db).await.unwrap();
    assert_eq!(count, 2);
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
    drop(service);
    db.close().await.unwrap();
    assert_restarted_decision(directory.path()).await;
}

async fn assert_restarted_decision(path: &std::path::Path) {
    let restarted = PersistentControlService::start(
        Store::open(path.join("state.sqlite3")).await.unwrap(),
        100,
    )
    .await
    .unwrap();
    let ControlResponse::Task(task) = restarted
        .handle(ControlRequest::GetTask {
            task_id: "controlled".into(),
        })
        .await
        .unwrap()
    else {
        panic!("task");
    };
    assert_eq!(task.aggregate.version, 2);
    assert_eq!(task.aggregate.decisions.len(), 1);
    let decision = &task.aggregate.decisions[0];
    assert_eq!(decision.question, "<script>Which format?</script>");
    assert_eq!(
        decision.resolution.as_ref().unwrap().rationale,
        "Use plain text."
    );
    assert_eq!(
        decision.resolution.as_ref().unwrap().resolved_by.0,
        "local-board-operator"
    );
}
