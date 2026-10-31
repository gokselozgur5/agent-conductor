//! Runs `open_with` against a fake daemon that speaks the real socket framing.

use std::sync::{Arc, Mutex};

use conductor_protocol::{ErrorBody, ErrorCode, ResponseEnvelope};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

type Answer = Box<dyn Fn(&Request) -> Result<Response, String> + Send + Sync>;

struct Fake {
    socket: PathBuf,
    seen: Arc<Mutex<Vec<Request>>>,
    _dir: tempfile::TempDir,
}

fn fake_daemon(answer: Answer) -> Fake {
    let dir = tempfile::Builder::new()
        .prefix("cc")
        .tempdir_in("/tmp")
        .expect("temp dir");
    let socket = dir.path().join("s.sock");
    assert!(socket.as_os_str().len() < 100);
    let listener = tokio::net::UnixListener::bind(&socket).expect("bind");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut raw = Vec::new();
            stream.read_to_end(&mut raw).await.expect("read request");
            let request: RequestEnvelope = serde_json::from_slice(&raw).expect("request json");
            let reply = match answer(&request.payload) {
                Ok(response) => ResponseEnvelope::success(&request, response),
                Err(message) => ResponseEnvelope::failure(
                    Some(&request),
                    ErrorBody::new(ErrorCode::Validation, message),
                ),
            };
            record.lock().expect("lock").push(request.payload);
            let body = serde_json::to_vec(&reply).expect("response json");
            stream.write_all(&body).await.expect("write response");
            stream.shutdown().await.expect("shutdown");
        }
    });
    Fake {
        socket,
        seen,
        _dir: dir,
    }
}

fn operator_message() -> Value {
    json!({"sequence": 1, "id": "m1", "agent_id": null, "parent": null,
        "body": "hello history", "routing": [], "status": "replied"})
}

fn with_history(request: &Request) -> Result<Response, String> {
    match request {
        Request::ConversationMessages { .. } => Ok(Response::ConversationMessages {
            messages: vec![operator_message()],
            next_after: None,
        }),
        other => Err(format!("unexpected {other:?}")),
    }
}

async fn run(daemon: &Fake, input: Vec<Line>) -> (Result<i32, String>, String) {
    let (sender, lines) = tokio::sync::mpsc::channel(16);
    for line in input {
        sender.send(line).await.expect("queue input");
    }
    drop(sender);
    let mut out = Vec::new();
    let result = open_with(&daemon.socket, "c1".into(), lines, &mut out).await;
    (result, String::from_utf8(out).expect("utf8"))
}

fn typed(texts: &[&str]) -> Vec<Line> {
    texts.iter().map(|text| Ok((*text).to_owned())).collect()
}

#[tokio::test]
async fn history_is_printed_before_any_input_is_handled() {
    let daemon = fake_daemon(Box::new(with_history));
    let (result, out) = run(&daemon, typed(&["/tree", "/quit"])).await;
    assert_eq!(result, Ok(0));
    let history = out.find("#1 you").expect("history printed");
    let tree = out.find("you: hello history").expect("tree printed");
    assert!(history < tree, "{out}");
}

#[tokio::test]
async fn quit_as_first_input_still_shows_history() {
    let daemon = fake_daemon(Box::new(with_history));
    let (result, out) = run(&daemon, typed(&["/quit"])).await;
    assert_eq!(result, Ok(0));
    assert!(out.contains("hello history"), "{out}");
}

#[tokio::test]
async fn a_failed_poll_keeps_the_window_open() {
    let daemon = fake_daemon(Box::new(|_| Err("database busy".into())));
    let (result, _) = run(&daemon, typed(&["/unknown", "/quit"])).await;
    assert_eq!(result, Ok(0));
    assert!(!daemon.seen.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn a_stdin_read_error_is_not_eof() {
    let daemon = fake_daemon(Box::new(with_history));
    let broken = std::io::Error::new(std::io::ErrorKind::InvalidData, "bad bytes");
    let (result, _) = run(&daemon, vec![Err(broken)]).await;
    let message = result.expect_err("must fail");
    assert!(message.contains("reading input failed"), "{message}");
}

#[tokio::test]
async fn clean_eof_exits_zero() {
    let daemon = fake_daemon(Box::new(with_history));
    let (result, _) = run(&daemon, vec![]).await;
    assert_eq!(result, Ok(0));
}
