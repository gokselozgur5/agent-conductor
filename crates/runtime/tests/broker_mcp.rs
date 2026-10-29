#![cfg(unix)]
use conductor_runtime::{
    BrokerError, BrokerFileResult, BrokerFilesystem, BrokerIntent, BrokerJournal, BrokerMcpError,
    BrokerScope, MAX_BROKER_FRAME, continuous_millis, serve_broker_mcp, serve_broker_stream,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, DuplexStream},
    net::UnixStream,
    sync::watch,
};
const WAIT: Duration = Duration::from_secs(2);
#[derive(Default)]
struct Journal(AtomicUsize);
impl BrokerJournal for Journal {
    fn admit(&self, _: &BrokerIntent) -> Result<(), BrokerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn complete(&self, _: &BrokerIntent, _: &BrokerFileResult) -> Result<(), BrokerError> {
        Ok(())
    }
}
fn scope() -> BrokerScope {
    BrokerScope {
        authority_id: "a".repeat(64),
        boot_generation: 1,
        invocation_id: uuid::Uuid::now_v7().to_string(),
        task_id: "task".into(),
        attempt_id: "attempt".into(),
        lease_epoch: 1,
    }
}
async fn send(io: &mut BufReader<DuplexStream>, value: Value) {
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    io.get_mut().write_all(&bytes).await.unwrap();
}
async fn call(io: &mut BufReader<DuplexStream>, value: Value) -> Value {
    send(io, value).await;
    let mut line = String::new();
    tokio::time::timeout(WAIT, io.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    serde_json::from_str(&line).unwrap()
}
async fn init(io: &mut BufReader<DuplexStream>) {
    let result=call(io,json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"future-unsupported","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}})).await;
    assert_eq!(result["result"]["protocolVersion"], "2025-06-18");
    send(
        io,
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await;
}
fn write(id: u64, args: &Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"conductor_write_text","arguments":args}})
}
fn args(text: &str) -> Value {
    json!({"path":"answer","contents":text,"expected_sha256":null})
}

#[tokio::test]
async fn mcp_real_create_read_replace_and_eof_close() {
    let root = tempfile::tempdir().unwrap();
    let scope = scope();
    let broker = BrokerFilesystem::new(
        &root.path().canonicalize().unwrap(),
        scope.clone(),
        continuous_millis().unwrap() + 60_000,
    )
    .unwrap();
    let journal = Arc::new(Journal::default());
    let sink = journal.clone();
    let (server, proxy) = UnixStream::pair().unwrap();
    let (_cancel, rx) = watch::channel(false);
    let host = tokio::spawn(serve_broker_stream(server, broker, sink, rx, WAIT, WAIT));
    let (client, stdio) = tokio::io::duplex(MAX_BROKER_FRAME * 2);
    let (input, output) = tokio::io::split(stdio);
    let task = tokio::spawn(serve_broker_mcp(input, output, proxy, scope, WAIT));
    let mut io = BufReader::new(client);
    init(&mut io).await;
    let listing = call(
        &mut io,
        json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
    )
    .await;
    assert_eq!(listing["result"]["tools"].as_array().unwrap().len(), 2);
    assert_eq!(
        listing["result"]["tools"][1]["inputSchema"]["required"],
        json!(["path", "contents", "expected_sha256"])
    );
    let first = call(&mut io, write(2, &args("first\nline"))).await;
    assert_eq!(first["result"]["isError"], false);
    let read=call(&mut io,json!({"jsonrpc":"2.0","id":"read","method":"tools/call","params":{"name":"conductor_read_text","arguments":{"path":"answer"}}})).await;
    let result: Value =
        serde_json::from_str(read["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(result["contents"], "first\nline");
    let mut replacement = args("second");
    replacement["expected_sha256"] = result["sha256"].clone();
    assert_eq!(
        call(&mut io, write(3, &replacement)).await["result"]["isError"],
        false
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("answer")).unwrap(),
        "second"
    );
    assert_eq!(journal.0.load(Ordering::SeqCst), 3);
    drop(io);
    assert_eq!(task.await.unwrap(), Ok(()));
    assert_eq!(host.await.unwrap().unwrap().completed_sequence, Some(3));
}

#[tokio::test]
async fn invalid_arguments_never_reach_broker_and_duplicate_id_closes() {
    let (mut peer, proxy) = UnixStream::pair().unwrap();
    let (client, stdio) = tokio::io::duplex(MAX_BROKER_FRAME * 2);
    let (input, output) = tokio::io::split(stdio);
    let task = tokio::spawn(serve_broker_mcp(input, output, proxy, scope(), WAIT));
    let mut io = BufReader::new(client);
    assert_eq!(
        call(&mut io, write(10, &args("early"))).await["error"]["code"],
        -32601
    );
    init(&mut io).await;
    let bad = [
        json!({"path":"answer","contents":"missing explicit hash"}),
        json!({"path":"../outside","contents":"bad","expected_sha256":null}),
        json!({"path":"answer","contents":"bad","expected_sha256":null,"scope":"forged"}),
        json!({"path":"answer","contents":"bad","expected_sha256":"invalid"}),
    ];
    for (id, arg) in (1_u64..).zip(bad) {
        assert_eq!(
            call(&mut io, write(id, &arg)).await["error"]["code"],
            -32602
        );
    }
    send(&mut io, write(1, &args("replayed"))).await;
    assert_eq!(task.await.unwrap(), Err(BrokerMcpError::Invalid));
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    assert!(bytes.is_empty());
}

#[tokio::test]
async fn malformed_partial_oversized_and_notification_calls_have_no_effect() {
    let cases = vec![
        b"[]\n".to_vec(),
        b"{\n".to_vec(),
        b"{\"jsonrpc\":\"2.0\"}".to_vec(),
        vec![b'x'; MAX_BROKER_FRAME + 1],
        b"{\"jsonrpc\":\"2.0\",\"method\":\"tools/call\",\"params\":{}}\n".to_vec(),
    ];
    for bytes in cases {
        let (mut peer, proxy) = UnixStream::pair().unwrap();
        let (mut client, stdio) = tokio::io::duplex(MAX_BROKER_FRAME * 2);
        let (input, output) = tokio::io::split(stdio);
        let task = tokio::spawn(serve_broker_mcp(input, output, proxy, scope(), WAIT));
        client.write_all(&bytes).await.unwrap();
        client.shutdown().await.unwrap();
        assert_eq!(task.await.unwrap(), Err(BrokerMcpError::Invalid));
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty());
    }
}

#[tokio::test]
async fn bad_broker_replies_close_without_retry_or_success() {
    for reply in [
        json!({"version":1,"sequence":99,"result":{"type":"written","sha256":"a".repeat(64)}})
            .to_string()
            + "\n",
        "not json\n".into(),
        "partial".into(),
        json!({"version":1,"sequence":1,"result":{"type":"written","sha256":"a".repeat(64)}})
            .to_string()
            + "\n",
    ] {
        let (peer, proxy) = UnixStream::pair().unwrap();
        let (client, stdio) = tokio::io::duplex(4096);
        let (input, output) = tokio::io::split(stdio);
        let task = tokio::spawn(serve_broker_mcp(input, output, proxy, scope(), WAIT));
        let mut io = BufReader::new(client);
        init(&mut io).await;
        let server = tokio::spawn(async move {
            let mut peer = BufReader::new(peer);
            let mut line = String::new();
            peer.read_line(&mut line).await.unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(request["sequence"], 1);
            peer.get_mut().write_all(reply.as_bytes()).await.unwrap();
            peer.get_mut().shutdown().await.unwrap();
            let mut rest = Vec::new();
            peer.read_to_end(&mut rest).await.unwrap();
            assert!(rest.is_empty());
        });
        let result = call(&mut io, write(1, &args("data"))).await;
        assert_eq!(result["result"]["isError"], true);
        assert_eq!(task.await.unwrap(), Err(BrokerMcpError::Broker));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn broker_timeout_closes_channel_without_retry() {
    let (peer, proxy) = UnixStream::pair().unwrap();
    let (client, stdio) = tokio::io::duplex(4096);
    let (input, output) = tokio::io::split(stdio);
    let task = tokio::spawn(serve_broker_mcp(
        input,
        output,
        proxy,
        scope(),
        Duration::from_millis(100),
    ));
    let mut io = BufReader::new(client);
    init(&mut io).await;
    send(&mut io, write(1, &args("data"))).await;
    let mut peer = BufReader::new(peer);
    let mut line = String::new();
    peer.read_line(&mut line).await.unwrap();
    assert!(!line.is_empty());
    assert_eq!(task.await.unwrap(), Err(BrokerMcpError::Broker));
    let mut rest = Vec::new();
    peer.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty());
}

#[tokio::test]
async fn live_proxy_cannot_write_after_host_revocation() {
    let root = tempfile::tempdir().unwrap();
    let scope = scope();
    let broker = BrokerFilesystem::new(
        &root.path().canonicalize().unwrap(),
        scope.clone(),
        continuous_millis().unwrap() + 60_000,
    )
    .unwrap();
    let journal = Arc::new(Journal::default());
    let sink = journal.clone();
    let (server, proxy) = UnixStream::pair().unwrap();
    let (cancel, rx) = watch::channel(false);
    let host = tokio::spawn(serve_broker_stream(server, broker, sink, rx, WAIT, WAIT));
    let (client, stdio) = tokio::io::duplex(4096);
    let (input, output) = tokio::io::split(stdio);
    let task = tokio::spawn(serve_broker_mcp(input, output, proxy, scope, WAIT));
    let mut io = BufReader::new(client);
    init(&mut io).await;
    cancel.send(true).unwrap();
    assert_eq!(host.await.unwrap().unwrap().completed_sequence, Some(0));
    assert!(!task.is_finished());
    assert_eq!(
        call(&mut io, write(1, &args("late write"))).await["result"]["isError"],
        true
    );
    assert_eq!(task.await.unwrap(), Err(BrokerMcpError::Broker));
    assert!(!root.path().join("answer").exists());
    assert_eq!(journal.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn output_backpressure_and_input_idle_close_broker() {
    for idle in [false, true] {
        let (mut peer, proxy) = UnixStream::pair().unwrap();
        let (mut client, stdio) = tokio::io::duplex(32);
        let (input, output) = tokio::io::split(stdio);
        let task = tokio::spawn(serve_broker_mcp(
            input,
            output,
            proxy,
            scope(),
            Duration::from_millis(200),
        ));
        if !idle {
            client.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"fixture\",\"version\":\"1\"}}}\n").await.unwrap();
        }
        assert_eq!(task.await.unwrap(), Err(BrokerMcpError::Transport));
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty());
        if !idle {
            let mut output = Vec::new();
            client.read_to_end(&mut output).await.unwrap();
            assert!(!output.is_empty());
            assert!(!output.ends_with(b"\n"));
        }
    }
}

#[tokio::test]
async fn abort_inflight_proxy_closes_channel_and_never_replays() {
    let (peer, proxy) = UnixStream::pair().unwrap();
    let (client, stdio) = tokio::io::duplex(4096);
    let (input, output) = tokio::io::split(stdio);
    let task = tokio::spawn(serve_broker_mcp(input, output, proxy, scope(), WAIT));
    let mut io = BufReader::new(client);
    init(&mut io).await;
    send(&mut io, write(1, &args("possibly admitted"))).await;
    let mut peer = BufReader::new(peer);
    let mut line = String::new();
    peer.read_line(&mut line).await.unwrap();
    assert!(!line.is_empty());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    assert!(bytes.is_empty());
}
