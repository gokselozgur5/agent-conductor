#![cfg(unix)]
use conductor_runtime::{
    BROKER_HANDOFF_ENV, BrokerEndpoint, BrokerEndpointError, BrokerError, BrokerFileResult,
    BrokerFilesystem, BrokerIntent, BrokerJournal, BrokerScope, connect_broker_endpoint,
    continuous_millis, serve_broker_stream,
};
use serde_json::{Value, json};
use std::{
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::{ChildStdin, ChildStdout, Command},
    sync::watch,
};
const WAIT: Duration = Duration::from_secs(3);
fn private_tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir_in(std::env::temp_dir().canonicalize().unwrap())
        .unwrap()
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
fn path(value: &str) -> PathBuf {
    PathBuf::from(
        serde_json::from_str::<Value>(value).unwrap()["path"]
            .as_str()
            .unwrap(),
    )
}
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
#[tokio::test]
async fn authenticated_pair_is_single_use_and_preserves_stream_bytes() {
    let root = private_tempdir();
    let scope = scope();
    let endpoint = BrokerEndpoint::bind(root.path(), scope.clone()).unwrap();
    let value = endpoint.environment_value().unwrap();
    let path = path(&value);
    let server = tokio::spawn(endpoint.accept(WAIT));
    let (mut client, actual) = connect_broker_endpoint(&value, WAIT).await.unwrap();
    assert!(actual == scope);
    let mut accepted = server.await.unwrap().unwrap();
    assert!(!path.exists());
    client.write_all(b"private request").await.unwrap();
    let mut bytes = [0; 15];
    accepted.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"private request");
    assert!(connect_broker_endpoint(&value, WAIT).await.is_err());
}
#[tokio::test]
async fn wrong_capability_or_scope_consumes_endpoint_without_connection() {
    for field in ["token", "scope"] {
        let root = private_tempdir();
        let endpoint = BrokerEndpoint::bind(root.path(), scope()).unwrap();
        let value = endpoint.environment_value().unwrap();
        let path = path(&value);
        let mut wrong: Value = serde_json::from_str(&value).unwrap();
        if field == "token" {
            wrong["token"] = json!("0".repeat(64));
        } else {
            wrong["scope"]["lease_epoch"] = json!(2);
        }
        let server = tokio::spawn(endpoint.accept(WAIT));
        assert!(
            connect_broker_endpoint(&wrong.to_string(), WAIT)
                .await
                .is_err()
        );
        assert!(matches!(
            server.await.unwrap(),
            Err(BrokerEndpointError::Refused)
        ));
        assert!(!path.exists());
    }
}
#[tokio::test]
async fn private_parent_and_socket_identity_are_required() {
    let root = private_tempdir();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(BrokerEndpoint::bind(root.path(), scope()).is_err());
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = BrokerEndpoint::bind(root.path(), scope()).unwrap();
    let value = endpoint.environment_value().unwrap();
    let socket = path(&value);
    std::fs::remove_file(&socket).unwrap();
    std::fs::write(&socket, b"replacement canary").unwrap();
    assert!(connect_broker_endpoint(&value, WAIT).await.is_err());
    drop(endpoint);
    assert_eq!(std::fs::read(socket).unwrap(), b"replacement canary");
}
#[tokio::test]
async fn stalled_authentication_and_aborted_accept_remove_only_owned_socket() {
    let root = private_tempdir();
    let endpoint = BrokerEndpoint::bind(root.path(), scope()).unwrap();
    let value = endpoint.environment_value().unwrap();
    let socket = path(&value);
    let server = tokio::spawn(endpoint.accept(Duration::from_millis(100)));
    let mut peer = UnixStream::connect(&socket).await.unwrap();
    peer.write_all(&[1, 2]).await.unwrap();
    assert!(matches!(
        server.await.unwrap(),
        Err(BrokerEndpointError::Transport)
    ));
    assert!(!socket.exists());
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    assert!(bytes.is_empty());
    let endpoint = BrokerEndpoint::bind(root.path(), scope()).unwrap();
    let socket = path(&endpoint.environment_value().unwrap());
    let server = tokio::spawn(endpoint.accept(WAIT));
    tokio::task::yield_now().await;
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    assert!(!socket.exists());
}
async fn send(input: &mut ChildStdin, value: &Value) {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    input.write_all(&bytes).await.unwrap();
}
async fn call(input: &mut ChildStdin, output: &mut BufReader<ChildStdout>, value: &Value) -> Value {
    send(input, value).await;
    let mut line = String::new();
    tokio::time::timeout(WAIT, output.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    serde_json::from_str(&line).unwrap()
}
async fn initialize(input: &mut ChildStdin, output: &mut BufReader<ChildStdout>) {
    let result=call(input,output,&json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}})).await;
    assert_eq!(result["result"]["protocolVersion"], "2025-06-18");
    send(
        input,
        &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await;
}
#[tokio::test]
async fn actual_proxy_stdio_writes_reads_and_closes_broker_on_eof() {
    let root = private_tempdir();
    let directory = private_tempdir();
    let scope = scope();
    let endpoint = BrokerEndpoint::bind(directory.path(), scope.clone()).unwrap();
    let handoff = endpoint.environment_value().unwrap();
    let broker =
        BrokerFilesystem::new(root.path(), scope, continuous_millis().unwrap() + 60_000).unwrap();
    let sink = Arc::new(Journal::default());
    let journal = sink.clone();
    let (_cancel, rx) = watch::channel(false);
    let host = tokio::spawn(async move {
        let stream = endpoint.accept(WAIT).await.unwrap();
        serve_broker_stream(stream, broker, sink, rx, WAIT, WAIT)
            .await
            .unwrap()
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_conductor-file-proxy"))
        .process_group(0)
        .env_clear()
        .env(BROKER_HANDOFF_ENV, &handoff)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    initialize(&mut input, &mut output).await;
    let write=call(&mut input,&mut output,&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"conductor_write_text","arguments":{"path":"answer","contents":"real proxy\n","expected_sha256":null}}})).await;
    assert_eq!(write["result"]["isError"], false);
    let read=call(&mut input,&mut output,&json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"conductor_read_text","arguments":{"path":"answer"}}})).await;
    let result: Value =
        serde_json::from_str(read["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(result["contents"], "real proxy\n");
    drop(input);
    let result = tokio::time::timeout(WAIT, child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(result.status.success());
    assert!(result.stderr.is_empty());
    assert_eq!(host.await.unwrap().completed_sequence, Some(2));
    assert_eq!(journal.0.load(Ordering::SeqCst), 2);
    assert_eq!(
        std::fs::read(root.path().join("answer")).unwrap(),
        b"real proxy\n"
    );
}
#[tokio::test]
async fn actual_live_proxy_loses_write_capability_after_host_stop() {
    let root = private_tempdir();
    let directory = private_tempdir();
    let scope = scope();
    let endpoint = BrokerEndpoint::bind(directory.path(), scope.clone()).unwrap();
    let handoff = endpoint.environment_value().unwrap();
    let broker =
        BrokerFilesystem::new(root.path(), scope, continuous_millis().unwrap() + 60_000).unwrap();
    let sink = Arc::new(Journal::default());
    let journal = sink.clone();
    let (cancel, rx) = watch::channel(false);
    let host = tokio::spawn(async move {
        let stream = endpoint.accept(WAIT).await.unwrap();
        serve_broker_stream(stream, broker, sink, rx, WAIT, WAIT)
            .await
            .unwrap()
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_conductor-file-proxy"))
        .process_group(0)
        .env_clear()
        .env(BROKER_HANDOFF_ENV, &handoff)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    initialize(&mut input, &mut output).await;
    let pid = nix::unistd::Pid::from_raw(i32::try_from(child.id().unwrap()).unwrap());
    assert_eq!(nix::unistd::getpgid(Some(pid)).unwrap(), pid);
    cancel.send(true).unwrap();
    assert_eq!(host.await.unwrap().completed_sequence, Some(0));
    assert!(child.try_wait().unwrap().is_none());
    let result=call(&mut input,&mut output,&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"conductor_write_text","arguments":{"path":"late","contents":"must not appear","expected_sha256":null}}})).await;
    assert_eq!(result["result"]["isError"], true);
    drop(input);
    let result = tokio::time::timeout(WAIT, child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(!result.status.success());
    assert_eq!(result.stderr, b"file proxy connection or protocol failed\n");
    assert!(!root.path().join("late").exists());
    assert_eq!(journal.0.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn proxy_missing_or_malformed_environment_has_no_protocol_output() {
    for value in ["", "private sentinel invalid handoff"] {
        let result = Command::new(env!("CARGO_BIN_EXE_conductor-file-proxy"))
            .env_clear()
            .env(BROKER_HANDOFF_ENV, value)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert_eq!(result.stderr, b"file proxy connection or protocol failed\n");
    }
}

#[tokio::test]
#[ignore = "requires the explicitly pinned Codex executable; local provider refuses inference"]
async fn pinned_codex_discovers_real_proxy_tools_without_model_backend() {
    codex_probe("inventory").await;
}
#[tokio::test]
#[ignore = "requires the explicitly pinned Codex executable; deterministic local provider only"]
async fn pinned_codex_calls_real_proxy_tools_without_model_backend() {
    codex_probe("approved").await;
}
#[tokio::test]
#[ignore = "requires the explicitly pinned Codex executable; negative approval control"]
async fn pinned_codex_unapproved_call_never_reaches_broker() {
    codex_probe("denied").await;
}
async fn codex_probe(mode: &str) {
    let root = private_tempdir();
    let directory = private_tempdir();
    let scope = scope();
    let endpoint = BrokerEndpoint::bind(directory.path(), scope.clone()).unwrap();
    let handoff = endpoint.environment_value().unwrap();
    let broker =
        BrokerFilesystem::new(root.path(), scope, continuous_millis().unwrap() + 120_000).unwrap();
    let sink = Arc::new(Journal::default());
    let journal = sink.clone();
    let (_cancel, rx) = watch::channel(false);
    let wait = Duration::from_secs(30);
    let host = tokio::spawn(async move {
        let stream = endpoint.accept(wait).await.unwrap();
        serve_broker_stream(stream, broker, sink, rx, wait, WAIT)
            .await
            .unwrap()
    });
    let binary = std::env::var_os("CONDUCTOR_CODEX_BIN").expect("set exact pinned Codex binary");
    let output = std::env::var_os("CONDUCTOR_CODEX_EVIDENCE").map_or_else(
        || root.path().join("codex"),
        |path| PathBuf::from(path).join(mode),
    );
    let proxy = PathBuf::from(env!("CARGO_BIN_EXE_conductor-file-proxy"))
        .canonicalize()
        .unwrap();
    let policy = conductor_runtime::codex_broker_tool_options(
        &proxy,
        &conductor_runtime::binary_sha256(&proxy).unwrap(),
    )
    .unwrap();
    let child = Command::new("/usr/bin/python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/codex_proxy_inventory.py"
        ))
        .arg(binary)
        .arg(env!("CARGO_BIN_EXE_conductor-file-proxy"))
        .arg(output)
        .env_clear()
        .env("TMPDIR", std::env::var_os("TMPDIR").unwrap())
        .env(BROKER_HANDOFF_ENV, handoff)
        .env("CONDUCTOR_PROXY_CALLS", mode)
        .env(
            "CONDUCTOR_CODEX_BROKER_OPTIONS",
            serde_json::to_string(&policy).unwrap(),
        )
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        child.status.success(),
        "local Codex inventory fixture failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    let end = tokio::time::timeout(Duration::from_secs(35), host)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        end.reason,
        conductor_runtime::BrokerStreamReason::PeerClosed
    );
    let operations = if mode == "approved" { 2 } else { 0 };
    assert_eq!(end.completed_sequence, Some(operations));
    assert_eq!(
        journal.0.load(Ordering::SeqCst),
        usize::try_from(operations).unwrap()
    );
    if mode == "approved" {
        assert_eq!(
            std::fs::read(root.path().join("codex-answer")).unwrap(),
            b"native codex proxy\n"
        );
    } else {
        assert!(!root.path().join("codex-answer").exists());
    }
}
