use serde::{Deserialize, Serialize};

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }
    };
}

string_id!(TaskId);
string_id!(AttemptId);
string_id!(ActorId);
string_id!(CheckId);
string_id!(CorrelationId);
string_id!(IdempotencyKey);
string_id!(SourceIdentity);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Backlog,
    Ready,
    Claimed,
    Running,
    Paused,
    Verifying,
    Blocked,
    Completed,
    Cancelled,
}

impl TaskState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorRole {
    Operator,
    Worker,
    Verifier,
    System,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Actor {
    pub id: ActorId,
    pub role: ActorRole,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseStatus {
    Active,
    RecoveryPending,
    Ended,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Lease {
    pub epoch: u64,
    pub boot_generation: u64,
    pub deadline_monotonic_ms: u64,
    pub worker_deadline_monotonic_ms: u64,
    pub remaining_at_last_heartbeat_ms: u64,
    pub recovery_grace_ms: u64,
    pub status: LeaseStatus,
}

impl Lease {
    #[must_use]
    pub fn accepts_worker_mutation(&self, clock: ClockReading) -> bool {
        self.status == LeaseStatus::Active
            && self.boot_generation == clock.boot_generation
            && clock.monotonic_ms < self.worker_deadline_monotonic_ms
            && clock.monotonic_ms < self.deadline_monotonic_ms
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClockReading {
    pub boot_generation: u64,
    pub monotonic_ms: u64,
    pub wall_unix_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Attempt {
    pub id: AttemptId,
    pub worker: ActorId,
    pub lease: Lease,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequiredCheck {
    pub id: CheckId,
    pub description: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckResult {
    pub check_id: CheckId,
    pub passed: bool,
    pub source: SourceIdentity,
    pub recorded_by: ActorId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Verification {
    pub source: Option<SourceIdentity>,
    pub checks: Vec<CheckResult>,
    pub verdict: Option<Verdict>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Verdict {
    pub accepted: bool,
    pub source: SourceIdentity,
    pub verifier: ActorId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Blocker {
    pub kind: String,
    pub detail: String,
    pub resume_when: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub state: TaskState,
    pub version: u64,
    pub last_lease_epoch: u64,
    pub attempt: Option<Attempt>,
    pub blocker: Option<Blocker>,
    pub required_checks: Vec<RequiredCheck>,
    pub verification: Option<Verification>,
}

impl Task {
    #[must_use]
    pub fn new(id: TaskId, required_checks: Vec<RequiredCheck>) -> Self {
        Self {
            id,
            state: TaskState::Backlog,
            version: 0,
            last_lease_epoch: 0,
            attempt: None,
            blocker: None,
            required_checks,
            verification: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransitionRecord {
    pub actor: Actor,
    pub reason: String,
    pub correlation_id: CorrelationId,
    pub idempotency_key: IdempotencyKey,
    pub occurred_at_unix_ms: i64,
    pub from: TaskState,
    pub to: TaskState,
    pub resulting_version: u64,
}
