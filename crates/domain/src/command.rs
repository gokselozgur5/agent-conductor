use serde::{Deserialize, Serialize};

use crate::{
    Actor, ActorId, AttemptId, Blocker, CheckId, CorrelationId, IdempotencyKey, RequiredCheck,
    SourceIdentity,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub actor: Actor,
    pub expected_version: u64,
    pub correlation_id: CorrelationId,
    pub idempotency_key: IdempotencyKey,
    pub reason: String,
    pub kind: CommandKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommandKind {
    MarkReady,
    Claim {
        attempt_id: AttemptId,
        worker: ActorId,
        lease_duration_ms: u64,
        worker_margin_ms: u64,
        recovery_grace_ms: u64,
    },
    Start {
        attempt_id: AttemptId,
        lease_epoch: u64,
    },
    Heartbeat {
        attempt_id: AttemptId,
        lease_epoch: u64,
        lease_duration_ms: u64,
        worker_margin_ms: u64,
    },
    EnterRecovery,
    RecoverHeartbeat {
        attempt_id: AttemptId,
        lease_epoch: u64,
        lease_duration_ms: u64,
        worker_margin_ms: u64,
    },
    RequeueExpired,
    Pause,
    Block {
        blocker: Blocker,
    },
    Resume,
    BeginVerification {
        attempt_id: AttemptId,
        lease_epoch: u64,
    },
    BindVerificationSource {
        source: SourceIdentity,
    },
    RecordCheck {
        check_id: CheckId,
        passed: bool,
        source: SourceIdentity,
    },
    RecordVerdict {
        accepted: bool,
        source: SourceIdentity,
    },
    RejectVerification,
    Complete {
        observed_source: SourceIdentity,
    },
    AddRequiredCheck {
        check: RequiredCheck,
    },
    OpenDecision {
        id: String,
        question: String,
    },
    ResolveDecision {
        id: String,
        rationale: String,
    },
    Cancel,
}
