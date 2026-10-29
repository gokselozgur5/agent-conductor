#![cfg(unix)]
use conductor_runtime::{
    BrokerError, BrokerFileResult, BrokerFilesystem, BrokerIntent, BrokerJournal, BrokerOperation,
    BrokerRequest, BrokerScope, BrokerStreamReason, MAX_BROKER_FRAME, continuous_millis,
    serve_broker_stream, serve_owned_broker_stream,
};
use std::{
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::watch,
};

#[derive(Default)]
struct Journal {
    events: Mutex<Vec<&'static str>>,
}
impl BrokerJournal for Journal {
    fn admit(&self, _: &BrokerIntent) -> Result<(), BrokerError> {
        self.events.lock().unwrap().push("admit");
        Ok(())
    }
    fn complete(&self, _: &BrokerIntent, _: &BrokerFileResult) -> Result<(), BrokerError> {
        self.events.lock().unwrap().push("complete");
        Ok(())
    }
}
fn fixture() -> (tempfile::TempDir, BrokerFilesystem, BrokerRequest) {
    let root = tempfile::tempdir().unwrap();
    let request = BrokerRequest {
        version: 1,
        sequence: 1,
        scope: BrokerScope {
            authority_id: "a".repeat(64),
            boot_generation: 1,
            invocation_id: uuid::Uuid::now_v7().to_string(),
            task_id: "task".into(),
            attempt_id: "attempt".into(),
            lease_epoch: 1,
        },
        operation: BrokerOperation::WriteText {
            path: "answer".into(),
            expected_sha256: None,
            contents: "transport bytes".into(),
        },
    };
    let broker = BrokerFilesystem::new(
        &root.path().canonicalize().unwrap(),
        request.scope.clone(),
        continuous_millis().unwrap() + 60_000,
    )
    .unwrap();
    (root, broker, request)
}
fn encoded(request: &BrokerRequest) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(request).unwrap();
    bytes.push(b'\n');
    bytes
}
const WAIT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn real_stream_write_reply_read_and_eof_close_are_ordered() {
    let (root, broker, mut request) = fixture();
    let journal = Arc::new(Journal::default());
    let (server, client) = UnixStream::pair().unwrap();
    let (_cancel, rx) = watch::channel(false);
    let sink = journal.clone();
    let task = tokio::spawn(serve_broker_stream(server, broker, sink, rx, WAIT, WAIT));
    let (reader, mut writer) = client.into_split();
    let mut reader = BufReader::new(reader);
    writer.write_all(&encoded(&request)).await.unwrap();
    let mut line = String::new();
    tokio::time::timeout(WAIT, reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(value["sequence"], 1);
    assert_eq!(value["result"]["type"], "written");
    assert_eq!(*journal.events.lock().unwrap(), vec!["admit", "complete"]);
    assert_eq!(
        std::fs::read_to_string(root.path().join("answer")).unwrap(),
        "transport bytes"
    );
    request.sequence = 2;
    request.operation = BrokerOperation::ReadText {
        path: "answer".into(),
    };
    writer.write_all(&encoded(&request)).await.unwrap();
    line.clear();
    tokio::time::timeout(WAIT, reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(value["result"]["contents"], "transport bytes");
    writer.shutdown().await.unwrap();
    let end = tokio::time::timeout(WAIT, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(end.reason, BrokerStreamReason::PeerClosed);
    assert_eq!(end.completed_sequence, Some(2));
}

#[tokio::test]
async fn malformed_oversized_and_partial_frames_never_reach_admission() {
    for mode in ["json", "partial", "oversized", "unknown"] {
        let (root, broker, request) = fixture();
        let journal = Arc::new(Journal::default());
        let (server, mut client) = UnixStream::pair().unwrap();
        let (_cancel, rx) = watch::channel(false);
        let task = tokio::spawn(serve_broker_stream(
            server,
            broker,
            journal.clone(),
            rx,
            WAIT,
            WAIT,
        ));
        let bytes = match mode {
            "json" => b"{bad}\n".to_vec(),
            "partial" => b"{\"version\":1".to_vec(),
            "oversized" => vec![b'x'; MAX_BROKER_FRAME + 1],
            "unknown" => {
                let mut v = serde_json::to_value(&request).unwrap();
                v["execute_shell"] = true.into();
                let mut bytes = serde_json::to_vec(&v).unwrap();
                bytes.push(b'\n');
                bytes
            }
            _ => unreachable!(),
        };
        let _ = tokio::time::timeout(WAIT, client.write_all(&bytes))
            .await
            .unwrap();
        let _ = client.shutdown().await;
        let end = tokio::time::timeout(WAIT, task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(end.reason, BrokerStreamReason::InvalidFrame, "{mode}");
        assert_eq!(end.completed_sequence, Some(0));
        assert!(journal.events.lock().unwrap().is_empty());
        assert!(!root.path().join("answer").exists());
    }
}
#[tokio::test]
async fn idle_peer_timeout_and_cancel_sender_loss_close_admission() {
    for disconnected in [false, true] {
        let (_root, broker, _) = fixture();
        let (server, _client) = UnixStream::pair().unwrap();
        let (cancel, rx) = watch::channel(false);
        let task = tokio::spawn(serve_broker_stream(
            server,
            broker,
            Arc::new(Journal::default()),
            rx,
            Duration::from_millis(30),
            WAIT,
        ));
        if disconnected {
            drop(cancel);
        }
        let end = tokio::time::timeout(WAIT, task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            end.reason,
            if disconnected {
                BrokerStreamReason::Cancelled
            } else {
                BrokerStreamReason::TimedOut
            }
        );
        assert_eq!(end.completed_sequence, Some(0));
    }
}

struct PausedJournal {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    finished: mpsc::Sender<bool>,
    returned: std::sync::atomic::AtomicBool,
}
impl Drop for PausedJournal {
    fn drop(&mut self) {
        let _ = self
            .finished
            .send(self.returned.load(std::sync::atomic::Ordering::SeqCst));
    }
}
impl BrokerJournal for PausedJournal {
    fn admit(&self, _: &BrokerIntent) -> Result<(), BrokerError> {
        self.entered.send(()).unwrap();
        self.release.lock().unwrap().recv_timeout(WAIT).unwrap();
        self.returned
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn complete(&self, _: &BrokerIntent, _: &BrokerFileResult) -> Result<(), BrokerError> {
        panic!("revoked admission must not reach files")
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_admission_timeout_is_unknown_and_released_worker_cannot_write() {
    let (root, broker, request) = fixture();
    let (server, mut client) = UnixStream::pair().unwrap();
    let (cancel, rx) = watch::channel(false);
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (finished, finish) = mpsc::channel();
    let journal = Arc::new(PausedJournal {
        entered,
        release: Mutex::new(released),
        finished,
        returned: std::sync::atomic::AtomicBool::new(false),
    });
    let task = tokio::spawn(serve_broker_stream(
        server,
        broker,
        journal,
        rx,
        WAIT,
        Duration::from_millis(30),
    ));
    client.write_all(&encoded(&request)).await.unwrap();
    tokio::task::spawn_blocking(move || entry.recv_timeout(WAIT).unwrap())
        .await
        .unwrap();
    cancel.send(true).unwrap();
    let end = tokio::time::timeout(WAIT, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(end.reason, BrokerStreamReason::Cancelled);
    assert_eq!(end.completed_sequence, None);
    release.send(()).unwrap();
    tokio::task::spawn_blocking(move || assert!(finish.recv_timeout(WAIT).unwrap()))
        .await
        .unwrap();
    // The last journal is dropped only after the blocking operation leaves the
    // gate. The completion channel therefore observes more than callback entry.
    assert!(!root.path().join("answer").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborting_server_future_revokes_an_inflight_admission() {
    let (root, broker, request) = fixture();
    let (server, mut client) = UnixStream::pair().unwrap();
    let (_cancel, rx) = watch::channel(false);
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (finished, finish) = mpsc::channel();
    let task = tokio::spawn(serve_broker_stream(
        server,
        broker,
        Arc::new(PausedJournal {
            entered,
            release: Mutex::new(released),
            finished,
            returned: std::sync::atomic::AtomicBool::new(false),
        }),
        rx,
        WAIT,
        WAIT,
    ));
    client.write_all(&encoded(&request)).await.unwrap();
    tokio::task::spawn_blocking(move || entry.recv_timeout(WAIT).unwrap())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    tokio::task::spawn_blocking(move || assert!(finish.recv_timeout(WAIT).unwrap()))
        .await
        .unwrap();
    assert!(!root.path().join("answer").exists());
}

#[tokio::test]
async fn pipelined_duplicate_does_not_repeat_admission_or_change_written_bytes() {
    let (root, broker, mut request) = fixture();
    let journal = Arc::new(Journal::default());
    let (server, mut client) = UnixStream::pair().unwrap();
    let (_cancel, rx) = watch::channel(false);
    let task = tokio::spawn(serve_broker_stream(
        server,
        broker,
        journal.clone(),
        rx,
        WAIT,
        WAIT,
    ));
    let mut bytes = encoded(&request);
    request.operation = BrokerOperation::WriteText {
        path: "answer".into(),
        contents: "replayed alteration".into(),
        expected_sha256: None,
    };
    bytes.extend(encoded(&request));
    client.write_all(&bytes).await.unwrap();
    let end = tokio::time::timeout(WAIT, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(end.reason, BrokerStreamReason::OperationFailed);
    assert_eq!(end.completed_sequence, Some(1));
    assert_eq!(*journal.events.lock().unwrap(), vec!["admit", "complete"]);
    assert_eq!(
        std::fs::read_to_string(root.path().join("answer")).unwrap(),
        "transport bytes"
    );
}

#[tokio::test]
async fn unread_large_reply_times_out_after_result_recording() {
    use tokio::io::AsyncReadExt;
    let (root, broker, mut request) = fixture();
    let journal = Arc::new(Journal::default());
    std::fs::write(root.path().join("answer"), vec![0u8; 256 * 1024]).unwrap();
    request.operation = BrokerOperation::ReadText {
        path: "answer".into(),
    };
    let (server, mut client) = UnixStream::pair().unwrap();
    let (_cancel, rx) = watch::channel(false);
    let task = tokio::spawn(serve_broker_stream(
        server,
        broker,
        journal.clone(),
        rx,
        Duration::from_secs(2),
        WAIT,
    ));
    client.write_all(&encoded(&request)).await.unwrap();
    // Observe actual reply publication before withholding reads. A completed
    // operation can otherwise time out before the reply even starts, which does
    // not exercise output backpressure. Allow setup separately from settlement.
    let first = tokio::time::timeout(Duration::from_secs(10), client.read_u8())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, b'{');
    let end = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(end.reason, BrokerStreamReason::TimedOut);
    assert_eq!(end.completed_sequence, Some(1));
    assert_eq!(*journal.events.lock().unwrap(), vec!["admit", "complete"]);
    let mut partial = vec![first];
    tokio::time::timeout(WAIT, client.take(2 * 1024 * 1024).read_to_end(&mut partial))
        .await
        .unwrap()
        .unwrap();
    assert!(partial.last() != Some(&b'\n'));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operation_deadline_does_not_certify_a_stalled_journal() {
    let (root, broker, request) = fixture();
    let (server, mut client) = UnixStream::pair().unwrap();
    let (_cancel, rx) = watch::channel(false);
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (finished, finish) = mpsc::channel();
    let task = tokio::spawn(serve_broker_stream(
        server,
        broker,
        Arc::new(PausedJournal {
            entered,
            release: Mutex::new(released),
            finished,
            returned: std::sync::atomic::AtomicBool::new(false),
        }),
        rx,
        Duration::from_millis(100),
        Duration::from_millis(30),
    ));
    client.write_all(&encoded(&request)).await.unwrap();
    tokio::task::spawn_blocking(move || entry.recv_timeout(WAIT).unwrap())
        .await
        .unwrap();
    let end = tokio::time::timeout(WAIT, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(end.reason, BrokerStreamReason::TimedOut);
    assert_eq!(end.completed_sequence, None);
    release.send(()).unwrap();
    tokio::task::spawn_blocking(move || assert!(finish.recv_timeout(WAIT).unwrap()))
        .await
        .unwrap();
    assert!(!root.path().join("answer").exists());
}

#[tokio::test]
async fn separate_session_peer_survives_broker_stop_but_cannot_write() {
    separate_session_control(0).await;
}

#[tokio::test]
async fn separate_session_delayed_start_preserves_write_revocation() {
    separate_session_control(3).await;
}

async fn separate_session_control(start_delay_secs: u64) {
    use std::{os::fd::OwnedFd, process::Stdio};
    let (root, broker, request) = fixture();
    let control = tempfile::tempdir().unwrap();
    let release = control.path().join("release-peer");
    let journal = Arc::new(Journal::default());
    let (server, client) = UnixStream::pair().unwrap();
    let (cancel, rx) = watch::channel(false);
    let peer = client.into_std().unwrap();
    peer.set_nonblocking(false).unwrap();
    let fd: OwnedFd = peer.into();
    let script = r"import os,socket,sys,time
from pathlib import Path
os.setsid()
peer=socket.socket(fileno=0);peer.settimeout(2)
time.sleep(int(sys.argv[3]))
print('ready',flush=True)
deadline=time.monotonic()+5
while not Path(sys.argv[1]).exists():
 if time.monotonic()>deadline:raise SystemExit(2)
 time.sleep(.01)
try:
 peer.sendall(sys.argv[2].encode())
 assert peer.recv(1)==b''
 print('closed',flush=True)
except (BrokenPipeError,ConnectionResetError):
 print('refused',flush=True)
";
    let wire_request = String::from_utf8(encoded(&request)).unwrap();
    let mut child = tokio::process::Command::new("/usr/bin/python3")
        .args(["-c", script])
        .arg(&release)
        .arg(wire_request)
        .arg(start_delay_secs.to_string())
        .stdin(Stdio::from(fd))
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    // Interpreter startup has a separate budget from broker I/O and revocation.
    tokio::time::timeout(Duration::from_secs(10), output.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(line.trim(), "ready");
    let task = tokio::spawn(serve_broker_stream(
        server,
        broker,
        journal.clone(),
        rx,
        WAIT,
        WAIT,
    ));
    cancel.send(true).unwrap();
    let end = tokio::time::timeout(WAIT, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(end.completed_sequence, Some(0));
    assert!(child.try_wait().unwrap().is_none());
    std::fs::write(&release, b"attempt write now").unwrap();
    line.clear();
    tokio::time::timeout(WAIT, output.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(line.trim(), "closed" | "refused"));
    assert!(
        tokio::time::timeout(WAIT, child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(journal.events.lock().unwrap().is_empty());
    assert!(!root.path().join("answer").exists());
}

#[tokio::test]
async fn owned_idle_accepts_later_work_and_bounds_partial_frames() {
    for partial in [false, true] {
        let (root, broker, request) = fixture();
        let journal = Arc::new(Journal::default());
        let (server, mut client) = UnixStream::pair().unwrap();
        let (_cancel, rx) = watch::channel(false);
        // This budget also covers durable filesystem operations, not only reads.
        // Keep a short partial-frame bound, but give the successful write the
        // normal I/O budget. Both cases idle longer than their own budget.
        let frame_budget = if partial {
            Duration::from_millis(100)
        } else {
            WAIT
        };
        let task = tokio::spawn(serve_owned_broker_stream(
            server,
            broker,
            journal.clone(),
            rx,
            frame_budget,
            WAIT,
        ));
        tokio::time::sleep(frame_budget + Duration::from_millis(150)).await;
        assert!(!task.is_finished());
        if partial {
            client.write_all(b"{").await.unwrap();
        } else {
            client.write_all(&encoded(&request)).await.unwrap();
            let mut reader = BufReader::new(client);
            let mut reply = String::new();
            tokio::time::timeout(WAIT, reader.read_line(&mut reply))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&reply).unwrap()["result"]["type"],
                "written"
            );
        }
        let end = tokio::time::timeout(WAIT, task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            end.reason,
            if partial {
                BrokerStreamReason::TimedOut
            } else {
                BrokerStreamReason::PeerClosed
            }
        );
        assert_eq!(end.completed_sequence, Some(u64::from(!partial)));
        assert_eq!(root.path().join("answer").exists(), !partial);
        assert_eq!(
            journal.events.lock().unwrap().len(),
            if partial { 0 } else { 2 }
        );
    }
}

#[tokio::test]
async fn owned_idle_observes_cancellation_and_sender_loss() {
    for sender_loss in [false, true] {
        let (_root, broker, _) = fixture();
        let (server, _client) = UnixStream::pair().unwrap();
        let (cancel, rx) = watch::channel(false);
        let task = tokio::spawn(serve_owned_broker_stream(
            server,
            broker,
            Arc::new(Journal::default()),
            rx,
            Duration::from_millis(100),
            WAIT,
        ));
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(!task.is_finished());
        if sender_loss {
            drop(cancel);
        } else {
            cancel.send(true).unwrap();
        }
        let end = tokio::time::timeout(WAIT, task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(end.reason, BrokerStreamReason::Cancelled);
        assert_eq!(end.completed_sequence, Some(0));
    }
}
