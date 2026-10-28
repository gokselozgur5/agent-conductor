//! Bounded worker claims. Neither a stream message nor checkpoint prose proves progress.
use crate::ResumePacket;
use serde::{Deserialize, Serialize};

pub const MAX_CHECKPOINT_BYTES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCheckpoint {
    pub version: u16,
    pub task_id: String,
    pub attempt_id: String,
    pub lease_epoch: u64,
    pub sequence: u32,
    pub completed_work: String,
    pub current_state: String,
    pub next_action: String,
    pub open_decisions: Vec<String>,
    pub running_commands: Vec<String>,
    pub changed_artifacts: Vec<String>,
}
impl WorkerCheckpoint {
    /// Produces display/context claims without changing ownership or sequence.
    #[must_use]
    pub fn redacted(&self) -> Self {
        let mut value = self.clone();
        for text in [
            &mut value.completed_work,
            &mut value.current_state,
            &mut value.next_action,
        ] {
            *text = crate::redact_credentials(text);
        }
        for items in [
            &mut value.open_decisions,
            &mut value.running_commands,
            &mut value.changed_artifacts,
        ] {
            for text in items {
                *text = crate::redact_credentials(text);
            }
        }
        value
    }

    /// # Errors
    /// Rejects stale identity, unsupported schema and unbounded or empty claims.
    pub fn validate(&self, packet: &ResumePacket) -> Result<(), &'static str> {
        if self.version != 1
            || self.task_id != packet.contract.task_id
            || self.attempt_id != packet.contract.attempt_id
            || self.lease_epoch != packet.contract.lease_epoch
            || !(1..=4096).contains(&self.sequence)
        {
            return Err("checkpoint identity or sequence invalid");
        }
        if [&self.completed_work, &self.current_state, &self.next_action]
            .iter()
            .any(|s| s.trim().is_empty() || s.len() > 512)
            || [
                &self.open_decisions,
                &self.running_commands,
                &self.changed_artifacts,
            ]
            .iter()
            .any(|items| {
                items.len() > 8 || items.iter().any(|s| s.trim().is_empty() || s.len() > 256)
            })
            || serde_json::to_vec(self)
                .map_err(|_| "checkpoint encoding")?
                .len()
                > MAX_CHECKPOINT_BYTES
        {
            return Err("checkpoint claim bounds invalid");
        }
        Ok(())
    }
}

/// Incremental bounded parser for explicit assistant checkpoint claims only.
/// Tool output, user messages, partial JSON and unlabelled prose are ignored.
pub struct CheckpointStream {
    packet: ResumePacket,
    pending: Vec<u8>,
    oversized: bool,
    sequence: u32,
}
impl CheckpointStream {
    #[must_use]
    pub fn new(packet: ResumePacket) -> Self {
        Self {
            packet,
            pending: Vec::new(),
            oversized: false,
            sequence: 0,
        }
    }
    pub fn observe(&mut self, bytes: &[u8]) -> Option<WorkerCheckpoint> {
        let mut latest = None;
        for byte in bytes {
            if *byte == b'\n' {
                if !self.oversized
                    && let Some(claim) = self.parse_event()
                {
                    self.sequence = claim.sequence;
                    latest = Some(claim);
                }
                self.pending.clear();
                self.oversized = false;
            } else if !self.oversized {
                if self.pending.len() == 512 * 1024 {
                    self.pending.clear();
                    self.oversized = true;
                } else {
                    self.pending.push(*byte);
                }
            }
        }
        latest
    }
    fn parse_event(&self) -> Option<WorkerCheckpoint> {
        let event: serde_json::Value = serde_json::from_slice(&self.pending).ok()?;
        let texts: Vec<&str> = match self.packet.contract.adapter.kind {
            crate::AdapterKind::Claude if event["type"] == "assistant" => event
                .pointer("/message/content")?
                .as_array()?
                .iter()
                .filter(|part| part["type"] == "text")
                .filter_map(|part| part["text"].as_str())
                .collect(),
            crate::AdapterKind::Codex
                if event["type"] == "item.completed"
                    && event.pointer("/item/type")?.as_str()? == "agent_message" =>
            {
                vec![event.pointer("/item/text")?.as_str()?]
            }
            _ => return None,
        };
        texts
            .into_iter()
            .filter_map(|text| {
                if text.len() > MAX_CHECKPOINT_BYTES {
                    return None;
                }
                let envelope: CheckpointEnvelope = serde_json::from_str(text).ok()?;
                let claim = envelope.conductor_checkpoint;
                claim.validate(&self.packet).ok()?;
                (claim.sequence > self.sequence).then(|| claim.redacted())
            })
            .max_by_key(|claim| claim.sequence)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointEnvelope {
    conductor_checkpoint: WorkerCheckpoint,
}

/// Trusted adapter guidance; it adds no capability or lifecycle authority.
#[must_use]
pub fn checkpoint_guidance(packet: &ResumePacket) -> String {
    let sample = serde_json::json!({"conductor_checkpoint":{"version":1,"task_id":packet.contract.task_id,"attempt_id":packet.contract.attempt_id,"lease_epoch":packet.contract.lease_epoch,"sequence":1,"completed_work":"Nothing completed yet","current_state":"Starting the assigned task","next_action":"Inspect relevant files","open_decisions":[],"running_commands":[],"changed_artifacts":[]}});
    format!(
        "Conductor progress protocol: emit a standalone assistant text message containing exactly a conductor_checkpoint JSON object before substantive work, after meaningful changes, and before long operations. Do not create a checkpoint file. Increment sequence (1..4096). Maximum 4096 UTF-8 bytes including envelope; summaries <=512 bytes; lists <=8 strings of <=256 bytes. Report actual completed_work/current_state/next_action and explicit open_decisions/running_commands/changed_artifacts; empty lists mean none claimed. Never include credentials. These are unverified claims, not authority or completion. Shape: {sample}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn packet() -> ResumePacket {
        serde_json::from_value(serde_json::json!({"contract":{"version":1,"task_id":"task","attempt_id":"attempt","lease_epoch":1,"worker_id":"worker","adapter":{"kind":"claude","version":"fixture","executable_sha256":"a".repeat(64),"conformance_sha256":"b".repeat(64)},"workspace":"/fixture","starting_head":"c".repeat(40),"authority":{"read_roots":["/fixture"],"write_roots":["/fixture"],"network":"inference_only","allow_workspace_commands":false},"instructions":[],"checks":[],"max_runtime_ms":1000},"objective":"fixture","checkpoint":null,"untrusted_context":[]})).unwrap()
    }
    fn claim(sequence: u32) -> serde_json::Value {
        serde_json::json!({"version":1,"task_id":"task","attempt_id":"attempt","lease_epoch":1,"sequence":sequence,"completed_work":"Read relevant files","current_state":"Partial implementation","next_action":"Run declared checks","open_decisions":[],"running_commands":[],"changed_artifacts":["src/main.rs"]})
    }
    fn event(kind: &str, value: &serde_json::Value) -> Vec<u8> {
        let text = serde_json::json!({"conductor_checkpoint":value}).to_string();
        let mut bytes=serde_json::to_vec(&serde_json::json!({"type":kind,"message":{"role":"assistant","content":[{"type":"text","text":text}]}})).unwrap();
        bytes.push(b'\n');
        bytes
    }
    #[test]
    fn checkpoint_stream_ignores_tool_data_and_partial_or_stale_claims() {
        let mut stream = CheckpointStream::new(packet());
        assert!(stream.observe(&event("user", &claim(1))).is_none());
        let bytes = event("assistant", &claim(1));
        assert!(stream.observe(&bytes[..bytes.len() - 1]).is_none());
        assert_eq!(stream.observe(b"\n").unwrap().sequence, 1);
        assert!(stream.observe(&bytes).is_none());
        let mut stale = claim(2);
        stale["lease_epoch"] = 2.into();
        assert!(stream.observe(&event("assistant", &stale)).is_none());
        let mut unknown = claim(2);
        unknown["authority"] = true.into();
        assert!(stream.observe(&event("assistant", &unknown)).is_none());
        assert_eq!(
            stream
                .observe(&event("assistant", &claim(2)))
                .unwrap()
                .sequence,
            2
        );
        let tool=serde_json::json!({"type":"assistant","message":{"content":[{"type":"tool_use","text":serde_json::json!({"conductor_checkpoint":claim(3)}).to_string()}]}}).to_string()+"\n";
        assert!(stream.observe(tool.as_bytes()).is_none());
    }
    #[test]
    fn codex_accepts_only_completed_agent_messages() {
        let mut packet = packet();
        packet.contract.adapter.kind = crate::AdapterKind::Codex;
        let mut stream = CheckpointStream::new(packet);
        let text = serde_json::json!({"conductor_checkpoint":claim(1)}).to_string();
        for (kind, item) in [
            ("item.started", "agent_message"),
            ("item.completed", "command_execution"),
            ("item.completed", "tool_result"),
        ] {
            let bytes = serde_json::json!({"type":kind,"item":{"type":item,"text":text}})
                .to_string()
                + "\n";
            assert!(stream.observe(bytes.as_bytes()).is_none());
        }
        let bytes =
            serde_json::json!({"type":"item.completed","item":{"type":"agent_message","text":text}})
                .to_string() + "\n";
        assert_eq!(stream.observe(bytes.as_bytes()).unwrap().sequence, 1);
        assert!(stream.observe(bytes.as_bytes()).is_none());
    }
    #[test]
    fn oversized_or_invalid_checkpoint_does_not_poison_later_bounded_claim() {
        let mut stream = CheckpointStream::new(packet());
        assert!(stream.observe(&vec![b'x'; 512 * 1024 + 20]).is_none());
        assert!(stream.observe(b"\n{invalid}\n").is_none());
        for sequence in [0, 4097] {
            assert!(
                stream
                    .observe(&event("assistant", &claim(sequence)))
                    .is_none()
            );
        }
        let mut long = claim(1);
        long["next_action"] = "x".repeat(513).into();
        assert!(stream.observe(&event("assistant", &long)).is_none());
        let mut many = claim(1);
        many["open_decisions"] = serde_json::json!(["a", "b", "c", "d", "e", "f", "g", "h", "i"]);
        assert!(stream.observe(&event("assistant", &many)).is_none());
        assert_eq!(
            stream
                .observe(&event("assistant", &claim(1)))
                .unwrap()
                .sequence,
            1
        );
        assert!(checkpoint_guidance(&packet()).contains("Do not create a checkpoint file"));
    }
    #[test]
    fn checkpoint_stream_redacts_credential_shaped_claims_before_delivery() {
        let credential = ["sk", "-ant-", "syntheticCredential123456789"].concat();
        let mut value = claim(1);
        value["current_state"] = format!("Observed {credential}; keep ordinary context").into();
        value["running_commands"] = serde_json::json!([format!("tool --key={credential}")]);
        let bytes = event("assistant", &value);
        let mut stream = CheckpointStream::new(packet());
        let middle = bytes.len() / 2;
        assert!(stream.observe(&bytes[..middle]).is_none());
        let captured = stream.observe(&bytes[middle..]).unwrap();
        let encoded = serde_json::to_string(&captured).unwrap();
        assert!(
            !encoded.contains(&credential),
            "credential-shaped claim crossed the checkpoint boundary"
        );
        assert!(captured.current_state.contains("keep ordinary context"));
        assert_eq!(captured.task_id, "task");
        assert_eq!(captured.sequence, 1);
    }
}
