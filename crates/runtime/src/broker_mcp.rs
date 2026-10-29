//! MCP translation over a host-supplied private broker connection. No file handles,
//! socket-path discovery, subprocess execution or reconnect authority lives here.
use crate::{BrokerOperation, BrokerRequest, BrokerScope, MAX_BROKER_FRAME};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, time::Duration};
use tokio::{
    io::{
        AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
        BufReader,
    },
    net::UnixStream,
};

const MAX_REPLY: usize = 2 * 1024 * 1024;
const MAX_OUTPUT: usize = 4 * 1024 * 1024;
const PROTOCOL: &str = "2025-06-18";

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BrokerMcpError {
    #[error("invalid MCP frame or session state")]
    Invalid,
    #[error("MCP transport failed or timed out")]
    Transport,
    #[error("broker response unavailable or invalid; operation outcome unknown")]
    Broker,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    jsonrpc: String,
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    path: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteArgs {
    path: String,
    expected_sha256: Option<String>,
    contents: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    version: u16,
    sequence: u64,
    result: Value,
}

fn tools() -> Value {
    json!({"tools":[
        {"name":"conductor_read_text","description":"Read a relative UTF-8 file (up to 256 KiB) and its SHA-256 digest.",
         "inputSchema":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":false}},
        {"name":"conductor_write_text","description":"Create with expected_sha256 null; replace only with the exact digest from a prior read. UTF-8 contents up to 256 KiB. Parent directory must exist.",
         "inputSchema":{"type":"object","properties":{"path":{"type":"string"},"contents":{"type":"string"},"expected_sha256":{"type":["string","null"]}},"required":["path","contents","expected_sha256"],"additionalProperties":false}}
    ]})
}
fn response(id: &Value, result: &Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":result})
}
fn error(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}

async fn frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    limit: usize,
) -> Result<Option<Vec<u8>>, BrokerMcpError> {
    let mut bytes = Vec::new();
    reader
        .take(limit as u64 + 1)
        .read_until(b'\n', &mut bytes)
        .await
        .map_err(|_| BrokerMcpError::Transport)?;
    if bytes.is_empty() {
        return Ok(None);
    }
    if bytes.len() > limit || bytes.last() != Some(&b'\n') {
        return Err(BrokerMcpError::Invalid);
    }
    Ok(Some(bytes))
}
async fn send<W: AsyncWrite + Unpin>(
    output: &mut W,
    value: &Value,
    wait: Duration,
) -> Result<(), BrokerMcpError> {
    let mut bytes = serde_json::to_vec(value).map_err(|_| BrokerMcpError::Invalid)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_OUTPUT {
        return Err(BrokerMcpError::Invalid);
    }
    tokio::time::timeout(wait, async {
        output.write_all(&bytes).await?;
        output.flush().await
    })
    .await
    .map_err(|_| BrokerMcpError::Transport)?
    .map_err(|_| BrokerMcpError::Transport)
}
fn operation(params: &Value) -> Option<BrokerOperation> {
    let object = params.as_object()?;
    if object
        .keys()
        .any(|k| !matches!(k.as_str(), "name" | "arguments" | "_meta"))
    {
        return None;
    }
    let args = object.get("arguments")?.clone();
    match object.get("name")?.as_str()? {
        "conductor_read_text" => {
            let args: ReadArgs = serde_json::from_value(args).ok()?;
            Some(BrokerOperation::ReadText { path: args.path })
        }
        "conductor_write_text" => {
            // Require explicit create-only intent; omission must not silently become null.
            args.get("expected_sha256")?;
            let args: WriteArgs = serde_json::from_value(args).ok()?;
            Some(BrokerOperation::WriteText {
                path: args.path,
                expected_sha256: args.expected_sha256,
                contents: args.contents,
            })
        }
        _ => None,
    }
}
fn checked_result(value: &Value, operation: &BrokerOperation) -> Result<Value, BrokerMcpError> {
    let object = value.as_object().ok_or(BrokerMcpError::Broker)?;
    let hash = object
        .get("sha256")
        .and_then(Value::as_str)
        .ok_or(BrokerMcpError::Broker)?;
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(BrokerMcpError::Broker);
    }
    match operation {
        BrokerOperation::ReadText { .. } => {
            let text = object
                .get("contents")
                .and_then(Value::as_str)
                .ok_or(BrokerMcpError::Broker)?;
            if object.len() != 3
                || object.get("type") != Some(&json!("text"))
                || text.len() > 256 * 1024
                || format!("{:x}", Sha256::digest(text.as_bytes())) != hash
            {
                return Err(BrokerMcpError::Broker);
            }
        }
        BrokerOperation::WriteText { contents, .. } => {
            if object.len() != 2
                || object.get("type") != Some(&json!("written"))
                || format!("{:x}", Sha256::digest(contents.as_bytes())) != hash
            {
                return Err(BrokerMcpError::Broker);
            }
        }
    }
    Ok(json!({"content":[{"type":"text","text":value.to_string()}],"isError":false}))
}
async fn forward(
    stream: &mut BufReader<UnixStream>,
    request: &BrokerRequest,
    wait: Duration,
) -> Result<Value, BrokerMcpError> {
    let mut bytes = serde_json::to_vec(request).map_err(|_| BrokerMcpError::Invalid)?;
    bytes.push(b'\n');
    BrokerRequest::decode(&bytes).map_err(|_| BrokerMcpError::Invalid)?;
    tokio::time::timeout(wait, async {
        stream
            .get_mut()
            .write_all(&bytes)
            .await
            .map_err(|_| BrokerMcpError::Broker)?;
        let bytes = frame(stream, MAX_REPLY)
            .await
            .map_err(|_| BrokerMcpError::Broker)?
            .ok_or(BrokerMcpError::Broker)?;
        let reply: Reply = serde_json::from_slice(&bytes).map_err(|_| BrokerMcpError::Broker)?;
        if reply.version != 1 || reply.sequence != request.sequence {
            return Err(BrokerMcpError::Broker);
        }
        checked_result(&reply.result, &request.operation)
    })
    .await
    .map_err(|_| BrokerMcpError::Broker)?
}

/// Translate a single MCP session to an already-connected, invocation-bound broker.
/// Implements the 2025-06-18 handshake/tools subset; unknown versions negotiate
/// that version, never an unimplemented version. No automatic retry or reconnect.
/// Each successful call advances the private sequence; duplicate MCP IDs refuse.
/// EOF/error/drop closes the supplied broker stream, but is NOT a write fence.
/// An admitted operation may still finish; the host must settle its broker.
/// Requests are serial: a cancellation notification closes the session when read,
/// not an immediate interrupt of an in-flight call. Host revocation is independent.
/// The caller must provide isolated stdio and authenticated broker handoff. This
/// function does not launch Codex, construct an endpoint, or establish OS isolation.
/// # Errors
/// Invalid frames/state, exhausted IDs, stalled transports or uncertain broker results.
pub async fn serve_broker_mcp<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    input: R,
    output: W,
    stream: UnixStream,
    scope: BrokerScope,
    wait: Duration,
) -> Result<(), BrokerMcpError> {
    serve_mcp(input, output, stream, scope, wait, false).await
}

/// Translate MCP for an independently supervised broker. After initialization,
/// wait for input without spending the frame budget, while watching broker loss.
/// The broker owner must enforce lease expiry and cancellation independently.
/// Initial handshake, partial frames, calls and replies remain bounded.
/// # Errors
/// Invalid protocol, lost broker, unsolicited broker data or stalled frames.
pub async fn serve_owned_broker_mcp<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    input: R,
    output: W,
    stream: UnixStream,
    scope: BrokerScope,
    wait: Duration,
) -> Result<(), BrokerMcpError> {
    serve_mcp(input, output, stream, scope, wait, true).await
}

async fn owned_input_ready<R: AsyncBufRead + Unpin>(
    input: &mut R,
    broker: &mut BufReader<UnixStream>,
) -> Result<bool, BrokerMcpError> {
    tokio::select! {
        biased;
        // No request is outstanding: EOF, errors and unsolicited bytes all
        // invalidate this private connection.
        _ = broker.fill_buf() => Err(BrokerMcpError::Broker),
        ready = input.fill_buf() => Ok(!ready.map_err(|_| BrokerMcpError::Transport)?.is_empty()),
    }
}

async fn serve_mcp<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    input: R,
    mut output: W,
    stream: UnixStream,
    scope: BrokerScope,
    wait: Duration,
    allow_idle: bool,
) -> Result<(), BrokerMcpError> {
    if wait.is_zero() || wait > Duration::from_secs(30) {
        return Err(BrokerMcpError::Invalid);
    }
    let mut input = BufReader::new(input);
    let mut broker = BufReader::new(stream);
    let mut state = 0_u8;
    let mut sequence = 1;
    let mut ids = BTreeSet::new();
    loop {
        if allow_idle && state == 2 && !owned_input_ready(&mut input, &mut broker).await? {
            return Ok(());
        }
        let bytes = tokio::time::timeout(wait, frame(&mut input, MAX_BROKER_FRAME))
            .await
            .map_err(|_| BrokerMcpError::Transport)??;
        let Some(bytes) = bytes else {
            return Ok(());
        };
        let message: Message =
            serde_json::from_slice(&bytes).map_err(|_| BrokerMcpError::Invalid)?;
        if message.jsonrpc != "2.0" {
            return Err(BrokerMcpError::Invalid);
        }
        let Some(id) = message.id else {
            match message.method.as_str() {
                "notifications/initialized" if state == 1 => {
                    state = 2;
                    continue;
                }
                "notifications/cancelled" => return Ok(()),
                // Unknown notifications have no effect and must never execute tools.
                name if name.starts_with("notifications/") => continue,
                _ => return Err(BrokerMcpError::Invalid),
            }
        };
        if !(id.is_i64() || id.is_u64() || id.as_str().is_some_and(|s| s.len() <= 256))
            || ids.len() >= 100_000
            || !ids.insert(id.to_string())
        {
            return Err(BrokerMcpError::Invalid);
        }
        let result = match message.method.as_str() {
            "initialize"
                if state == 0
                    && message.params["protocolVersion"].is_string()
                    && message.params["capabilities"].is_object()
                    && message.params["clientInfo"]["name"].is_string()
                    && message.params["clientInfo"]["version"].is_string() =>
            {
                state = 1;
                response(
                    &id,
                    &json!({"protocolVersion":PROTOCOL,"capabilities":{"tools":{}},"serverInfo":{"name":"conductor-files","version":env!("CARGO_PKG_VERSION")}}),
                )
            }
            "ping" if state > 0 => response(&id, &json!({})),
            "tools/list" if state == 2 => {
                if message.params.get("cursor").is_some() {
                    error(&id, -32602, "pagination is not supported")
                } else {
                    response(&id, &tools())
                }
            }
            "tools/call" if state == 2 => {
                let Some(operation) = operation(&message.params) else {
                    send(
                        &mut output,
                        &error(&id, -32602, "unsupported tool or invalid arguments"),
                        wait,
                    )
                    .await?;
                    continue;
                };
                let request = BrokerRequest {
                    version: 1,
                    scope: scope.clone(),
                    sequence,
                    operation,
                };
                match forward(&mut broker, &request, wait).await {
                    Ok(result) => {
                        sequence += 1;
                        response(&id, &result)
                    }
                    Err(BrokerMcpError::Invalid) => error(&id, -32602, "invalid file operation"),
                    Err(_) => {
                        let reply = response(
                            &id,
                            &json!({"content":[{"type":"text","text":"Broker unavailable; operation outcome unknown. Session closed; do not retry."}],"isError":true}),
                        );
                        // Drop capability before attempting a possibly blocked MCP reply.
                        drop(broker);
                        send(&mut output, &reply, wait).await?;
                        return Err(BrokerMcpError::Broker);
                    }
                }
            }
            _ => error(&id, -32601, "method unavailable in this session state"),
        };
        send(&mut output, &result, wait).await?;
    }
}
