use super::*;
use conductor_protocol::{Request as ControlRequest, Response as ControlResponse};
use serde_json::{Value, json};
use sqlx::{Connection, SqliteConnection};

async fn append(db: &mut SqliteConnection, id: &str, task: Option<&str>, payload: &str) {
    sqlx::query("INSERT INTO audit_events(id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES(?,?,'operator','complete','PRIVATE_CORRELATION',?,0)")
        .bind(id).bind(task).bind(payload).execute(db).await.unwrap();
}
fn payload(task: &str) -> String {
    json!([task,{"actor":{"id":"operator","role":"operator"},"expected_version":10,
        "correlation_id":"PRIVATE_CORRELATION","idempotency_key":"PRIVATE_IDEMPOTENCY",
        "reason":"PRIVATE_REASON","kind":{"type":"complete","observed_source":"snapshot:fixture"}}])
    .to_string()
}
async fn page(address: SocketAddr, auth: &str, path: &str) -> Value {
    let raw = super::tests::request(address, &address.to_string(), path, auth).await;
    assert!(raw.starts_with("HTTP/1.1 200"), "{raw}");
    for private in [
        "PRIVATE_CORRELATION",
        "PRIVATE_REASON",
        "PRIVATE_IDEMPOTENCY",
        "UNRELATED",
    ] {
        assert!(!raw.contains(private));
    }
    serde_json::from_str(raw.split("\r\n\r\n").nth(1).unwrap()).unwrap()
}
#[tokio::test]
async fn evidence_pages_are_scoped_read_only_and_authenticated() {
    let directory = tempfile::tempdir().unwrap();
    let service = super::controls_tests::fixture(directory.path()).await;
    let mut db = SqliteConnection::connect(&format!(
        "sqlite:{}",
        directory.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    for n in 0..205 {
        append(
            &mut db,
            &format!("evidence-{n}"),
            Some("controlled"),
            &payload("controlled"),
        )
        .await;
    }
    append(&mut db, "unrelated", None, &payload("UNRELATED")).await;
    let token = "synthetic-evidence-credential-0000000";
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
    let path = "/api/evidence?task_id=controlled";
    assert!(
        super::tests::request(address, &address.to_string(), path, "")
            .await
            .starts_with("HTTP/1.1 401")
    );
    let mut before = None;
    let mut sequences = Vec::new();
    for expected in [100, 100, 5] {
        let query = format!(
            "{path}{}",
            before
                .as_ref()
                .map_or_else(String::new, |v| format!("&before={v}"))
        );
        let data = page(address, &auth, &query).await;
        let entries = data["entries"].as_array().unwrap();
        assert_eq!(entries.len(), expected);
        for entry in entries {
            assert_eq!(entry["details"]["recorded_completion"], true);
            sequences.push(entry["sequence"].as_str().unwrap().parse::<i64>().unwrap());
        }
        before = data["next_before"].as_str().map(str::to_owned);
        if sequences.len() == 100 {
            append(&mut db, "newer", Some("controlled"), &payload("controlled")).await;
        }
    }
    assert!(before.is_none());
    assert!(sequences.windows(2).all(|p| p[0] > p[1]));
    refusals(&mut db, address, &auth, path, &service).await;
    let ControlResponse::Task(task) = service
        .handle(ControlRequest::GetTask {
            task_id: "controlled".into(),
        })
        .await
        .unwrap()
    else {
        panic!("task")
    };
    assert_eq!(task.aggregate.version, 0);
    let state: String = sqlx::query_scalar("SELECT state FROM tasks WHERE id='controlled'")
        .fetch_one(&mut db)
        .await
        .unwrap();
    assert_eq!(state, "backlog");
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

async fn refusals(
    db: &mut SqliteConnection,
    address: SocketAddr,
    auth: &str,
    path: &str,
    service: &conductor_service::PersistentControlService,
) {
    for suffix in [
        "&before=0",
        "&before=-1",
        "&before=bad",
        "&before=9223372036854775808",
        "&unknown=1",
    ] {
        assert!(
            !super::tests::request(
                address,
                &address.to_string(),
                &format!("{path}{suffix}"),
                auth
            )
            .await
            .starts_with("HTTP/1.1 200")
        );
    }
    assert!(
        service
            .handle(ControlRequest::TaskEvidence {
                task_id: "missing".into(),
                before: None
            })
            .await
            .is_err()
    );
    append(
        db,
        "oversized",
        Some("controlled"),
        &json!({"private":"X".repeat(65537)}).to_string(),
    )
    .await;
    append(db, "malformed", Some("controlled"), "{}").await;
    append(db, "wrong-task", Some("controlled"), &payload("UNRELATED")).await;
    let data = page(address, auth, path).await;
    for row in &data["entries"].as_array().unwrap()[..3] {
        assert_eq!(row["details"]["availability"], "unavailable");
    }
}
