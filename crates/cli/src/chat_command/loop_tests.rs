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
    slow_daemon(answer, Box::new(|_| std::time::Duration::ZERO))
}

type Delay = Box<dyn Fn(&Request) -> std::time::Duration + Send + Sync>;

fn slow_daemon(answer: Answer, delay: Delay) -> Fake {
    let dir = tempfile::Builder::new()
        .prefix("cc")
        .tempdir_in("/tmp")
        .expect("temp dir");
    let socket = dir.path().join("s.sock");
    assert!(socket.as_os_str().len() < 100);
    let listener = tokio::net::UnixListener::bind(&socket).expect("bind");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&seen);
    let answer = Arc::new(answer);
    let delay = Arc::new(delay);
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let (answer, delay, record) = (answer.clone(), delay.clone(), record.clone());
            tokio::spawn(async move {
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
                record.lock().expect("lock").push(request.payload.clone());
                tokio::time::sleep(delay(&request.payload)).await;
                let body = serde_json::to_vec(&reply).expect("response json");
                let _ = stream.write_all(&body).await;
                let _ = stream.shutdown().await;
            });
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
        Request::ConversationRoom { .. } => Ok(Response::ConversationRoom {
            configuration: Some(room_config("lead", &["worker".into()])),
        }),
        other => Err(format!("unexpected {other:?}")),
    }
}

async fn run(daemon: &Fake, input: Vec<Line>) -> (Result<i32, String>, String) {
    run_every(daemon, input, std::time::Duration::from_secs(3600)).await
}

async fn run_every(
    daemon: &Fake,
    input: Vec<Line>,
    every: std::time::Duration,
) -> (Result<i32, String>, String) {
    let (sender, lines) = tokio::sync::mpsc::channel(16);
    for line in input {
        sender.send(line).await.expect("queue input");
    }
    drop(sender);
    let mut out = Vec::new();
    let result = open_with(&daemon.socket, "c1".into(), lines, &mut out, every).await;
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

#[tokio::test]
async fn input_stays_responsive_while_a_poll_is_in_flight() {
    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = Arc::clone(&polls);
    let daemon = slow_daemon(
        Box::new(with_history),
        Box::new(move |_| {
            if count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                std::time::Duration::ZERO
            } else {
                std::time::Duration::from_secs(30)
            }
        }),
    );
    let (sender, lines) = tokio::sync::mpsc::channel(16);
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let _ = sender.send(Ok("/quit".to_owned())).await;
    });
    let mut out = Vec::new();
    let every = std::time::Duration::from_millis(20);
    let finished = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        open_with(&daemon.socket, "c1".into(), lines, &mut out, every),
    )
    .await;
    assert_eq!(finished, Ok(Ok(0)));
}

#[tokio::test]
async fn a_sent_message_shows_without_waiting_for_the_next_tick() {
    let sent = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&sent);
    let daemon = fake_daemon(Box::new(move |request| match request {
        Request::SendConversationMessage { .. } => {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(Response::ConversationMessageSaved { message: json!({}) })
        }
        Request::RouteConversationMessage { .. } => Ok(Response::ConversationTurnQueued {
            message_id: "m".into(),
        }),
        Request::ConversationMessages { .. } if flag.load(std::sync::atomic::Ordering::SeqCst) => {
            let mut sent = operator_message();
            sent["sequence"] = json!(2);
            sent["body"] = json!("just typed");
            Ok(Response::ConversationMessages {
                messages: vec![operator_message(), sent],
                next_after: None,
            })
        }
        other => with_history(other),
    }));
    let (sender, lines) = tokio::sync::mpsc::channel(16);
    tokio::spawn(async move {
        let _ = sender.send(Ok("just typed".to_owned())).await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let _ = sender.send(Ok("/quit".to_owned())).await;
    });
    let mut out = Vec::new();
    let every = std::time::Duration::from_secs(3600);
    let result = open_with(&daemon.socket, "c1".into(), lines, &mut out, every).await;
    assert_eq!(result, Ok(0));
    assert!(String::from_utf8(out).expect("utf8").contains("#2 you"));
}

#[tokio::test]
async fn a_message_to_an_unknown_agent_saves_nothing() {
    let daemon = fake_daemon(Box::new(with_history));
    let (result, _) = run(&daemon, typed(&["@ghost hi", "/quit"])).await;
    assert_eq!(result, Ok(0));
    let seen = daemon.seen.lock().expect("lock");
    let saved = seen
        .iter()
        .any(|r| matches!(r, Request::SendConversationMessage { .. }));
    assert!(!saved, "{seen:?}");
    assert!(
        seen.iter()
            .any(|r| matches!(r, Request::ConversationRoom { .. }))
    );
}
