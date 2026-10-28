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
        post(address, &auth, "", &"x".repeat(4097))
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

async fn fixture(path: &std::path::Path) -> PersistentControlService {
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
