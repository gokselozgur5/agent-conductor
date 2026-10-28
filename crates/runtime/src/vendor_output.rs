//! Bounded interpretation of pinned CLI event streams. A candidate is untrusted
//! text bound to an attempt, never a task verdict or an authorization grant.
use crate::{AdapterKind, ProcessOutput, ResumePacket};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub struct VendorCandidate {
    pub attempt_id: String,
    pub packet_sha256: String,
    pub output_sha256: String,
    pub text: String,
    pub event_count: usize,
    pub failed_tools: usize,
    /// Observed restricted Claude tool envelope; not an OS fence by itself.
    pub file_only_tools: bool,
}
impl std::fmt::Debug for VendorCandidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendorCandidate")
            .field("attempt_id", &self.attempt_id)
            .field("output_sha256", &self.output_sha256)
            .field("event_count", &self.event_count)
            .field("failed_tools", &self.failed_tools)
            .finish_non_exhaustive()
    }
}
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum VendorOutputError {
    #[error("vendor process did not exit successfully")]
    Process,
    #[error("vendor output exceeds its bounds")]
    Bounds,
    #[error("vendor event stream is malformed or unsupported")]
    Protocol,
    #[error("vendor reported failure, interruption or budget exhaustion")]
    Failed,
    #[error("vendor event stream has no unique complete final response")]
    Incomplete,
}

/// Parses only the supported native event envelope. Tool output and model prose
/// are never recursively parsed into control events. Success still needs source
/// capture, independent checks and a distinct verifier in the service layer.
/// # Errors
/// Refuses failed processes, malformed/oversized/truncated streams, repeated
/// terminal events, missing final text and vendor failure/budget outcomes.
pub fn interpret_vendor_output(
    packet: &ResumePacket,
    output: &ProcessOutput,
) -> Result<VendorCandidate, VendorOutputError> {
    packet.validate().map_err(|_| VendorOutputError::Protocol)?;
    if output.exit_code != Some(0) {
        return Err(VendorOutputError::Process);
    }
    if output.stdout.len() > 2 * 1024 * 1024 || output.stderr.len() > 2 * 1024 * 1024 {
        return Err(VendorOutputError::Bounds);
    }
    if output.stdout.last() != Some(&b'\n') {
        return Err(VendorOutputError::Incomplete);
    }
    let mut state = StreamState::default();
    for line in output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        if line.len() > 512 * 1024 || state.events >= 10000 {
            return Err(VendorOutputError::Bounds);
        }
        if state.terminal {
            return Err(VendorOutputError::Incomplete);
        }
        let event: Value = serde_json::from_slice(line).map_err(|_| VendorOutputError::Protocol)?;
        if !event.is_object() {
            return Err(VendorOutputError::Protocol);
        }
        state.events += 1;
        match packet.contract.adapter.kind {
            AdapterKind::Claude => state.claude(&event)?,
            AdapterKind::Codex => state.codex(&event)?,
            AdapterKind::Fake => return Err(VendorOutputError::Protocol),
        }
    }
    if !state.terminal || state.text.trim().is_empty() {
        return Err(VendorOutputError::Incomplete);
    }
    if state.text.len() > 256 * 1024 {
        return Err(VendorOutputError::Bounds);
    }
    Ok(VendorCandidate {
        attempt_id: packet.contract.attempt_id.clone(),
        packet_sha256: packet.sha256().map_err(|_| VendorOutputError::Protocol)?,
        output_sha256: format!("{:x}", Sha256::digest(&output.stdout)),
        text: state.text,
        event_count: state.events,
        failed_tools: state.failed_tools,
        file_only_tools: state.tool_init == Some(true) && !state.non_file_tool,
    })
}
#[derive(Default)]
struct StreamState {
    events: usize,
    terminal: bool,
    text: String,
    failed_tools: usize,
    turn_started: bool,
    tool_init: Option<bool>,
    non_file_tool: bool,
}
impl StreamState {
    fn claude(&mut self, event: &Value) -> Result<(), VendorOutputError> {
        match event.get("type").and_then(Value::as_str) {
            Some("result") => {
                if event.get("subtype").and_then(Value::as_str) != Some("success")
                    || event.get("is_error").and_then(Value::as_bool) != Some(false)
                {
                    return Err(VendorOutputError::Failed);
                }
                self.text = event
                    .get("result")
                    .and_then(Value::as_str)
                    .ok_or(VendorOutputError::Incomplete)?
                    .into();
                self.terminal = true;
            }
            Some("user") => {
                if let Some(content) = event.pointer("/message/content").and_then(Value::as_array) {
                    self.failed_tools += content
                        .iter()
                        .filter(|part| {
                            part.get("type").and_then(Value::as_str) == Some("tool_result")
                                && part.get("is_error").and_then(Value::as_bool) == Some(true)
                        })
                        .count();
                }
            }
            Some("system") if event.get("subtype").and_then(Value::as_str) == Some("init") => {
                if self.tool_init.is_some() {
                    return Err(VendorOutputError::Protocol);
                }
                let tools = event
                    .get("tools")
                    .and_then(Value::as_array)
                    .ok_or(VendorOutputError::Protocol)?;
                self.tool_init =
                    Some(!tools.is_empty() && tools.iter().all(|tool| file_tool(tool.as_str())));
            }
            Some("assistant") => {
                if let Some(content) = event.pointer("/message/content").and_then(Value::as_array) {
                    for part in content {
                        if part.get("type").and_then(Value::as_str) == Some("tool_use") {
                            self.non_file_tool |=
                                !file_tool(part.get("name").and_then(Value::as_str));
                        }
                    }
                }
            }
            Some(
                "system" | "rate_limit_event" | "stream_event" | "tool_progress"
                | "tool_use_summary",
            ) => {}
            _ => return Err(VendorOutputError::Protocol),
        }
        Ok(())
    }
    fn codex(&mut self, event: &Value) -> Result<(), VendorOutputError> {
        match event.get("type").and_then(Value::as_str) {
            Some("thread.started" | "item.started" | "item.updated") => {}
            Some("turn.started") => {
                if self.turn_started {
                    return Err(VendorOutputError::Protocol);
                }
                self.turn_started = true;
            }
            Some("turn.completed") => {
                if !self.turn_started {
                    return Err(VendorOutputError::Incomplete);
                }
                self.terminal = true;
            }
            Some("turn.failed" | "error") => return Err(VendorOutputError::Failed),
            Some("item.completed") => {
                let item = event
                    .get("item")
                    .filter(|item| item.is_object())
                    .ok_or(VendorOutputError::Protocol)?;
                match item.get("type").and_then(Value::as_str) {
                    Some("agent_message") => {
                        self.text = item
                            .get("text")
                            .and_then(Value::as_str)
                            .ok_or(VendorOutputError::Protocol)?
                            .into();
                    }
                    Some("command_execution") => {
                        if item.get("exit_code").and_then(Value::as_i64) != Some(0) {
                            self.failed_tools += 1;
                        }
                    }
                    Some(
                        "reasoning" | "file_change" | "mcp_tool_call" | "web_search" | "todo_list",
                    ) => {}
                    _ => return Err(VendorOutputError::Protocol),
                }
            }
            _ => return Err(VendorOutputError::Protocol),
        }
        Ok(())
    }
}

fn file_tool(name: Option<&str>) -> bool {
    matches!(name, Some("Read" | "Write" | "Edit" | "Glob" | "Grep"))
}
