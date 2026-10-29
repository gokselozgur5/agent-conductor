use super::*;
use conductor_protocol::{RequestEnvelope, ResponseEnvelope, ResponseResult};
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct RecordingService(Arc<Mutex<Vec<ControlRequest>>>);
impl ControlService for RecordingService {
    async fn handle(
        &self,
        request: ControlRequest,
    ) -> Result<conductor_protocol::Response, conductor_protocol::ErrorBody> {
        self.0.lock().unwrap().push(request);
        Err(conductor_protocol::ErrorBody::new(
            conductor_protocol::ErrorCode::Conflict,
            "fixture conflict",
        ))
    }
}
async fn post(address: SocketAddr, headers: &str, body: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("POST /api/rpc HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}\r\n{body}", body.len()).as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn rpc_preserves_identity_and_errors_and_authenticates_before_dispatch() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let token = "synthetic-rpc-credential-000000000000";
    let board = OperatorBoard::bind(
        "127.0.0.1:0".parse().unwrap(),
        token.into(),
        RecordingService(Arc::clone(&calls)),
    )
    .await
    .unwrap();
    let address = board.address();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(board.run_until(async {
        let _ = stopped.await;
    }));
    let payload = ControlRequest::RegisterAgent {
        actor_id: "operator".into(),
        idempotency_key: "register-account-worker".into(),
        id: "account-worker".into(),
        adapter_kind: "claude".into(),
        protocol_fingerprint: "protocol".into(),
        capability_hash: "capability".into(),
        access_mode: "read_write".into(),
    };
    let mut request = RequestEnvelope::new(payload.clone());
    let body = serde_json::to_string(&request).unwrap();
    assert!(post(address, "", &body).await.starts_with("HTTP/1.1 401"));
    let auth = format!("Authorization: Bearer {token}\r\n");
    assert!(
        post(
            address,
            &format!("{auth}Origin: http://other.invalid\r\n"),
            &body
        )
        .await
        .starts_with("HTTP/1.1 403")
    );
    request.version = u16::MAX;
    let invalid = post(address, &auth, &serde_json::to_string(&request).unwrap()).await;
    let invalid: ResponseEnvelope =
        serde_json::from_str(invalid.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert!(matches!(invalid.result, ResponseResult::Error(_)));
    assert!(calls.lock().unwrap().is_empty());
    let result = post(address, &auth, &body).await;
    assert!(result.starts_with("HTTP/1.1 200"));
    let response: ResponseEnvelope =
        serde_json::from_str(result.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(response.request_message_id, Some(request.message_id));
    assert_eq!(response.correlation_id, request.correlation_id);
    assert!(
        matches!(response.result, ResponseResult::Error(error) if error.code == conductor_protocol::ErrorCode::Conflict)
    );
    assert_eq!(*calls.lock().unwrap(), vec![payload]);
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
