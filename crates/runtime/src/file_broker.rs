//! Broker protocol and in-process revocation ordering. Filesystem and durable
//! intent implementations are separate trusted ports, not supplied by the proxy.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

pub const MAX_BROKER_FRAME: usize = 1024 * 1024;
const MAX_TEXT: usize = 256 * 1024;
const MAX_OPERATIONS: u64 = 100_000;

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerScope {
    pub authority_id: String,
    pub boot_generation: u64,
    pub invocation_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub lease_epoch: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrokerOperation {
    ReadText {
        path: String,
    },
    /// None means create-only; replacement requires the exact prior file digest.
    WriteText {
        path: String,
        expected_sha256: Option<String>,
        contents: String,
    },
}

/// Deliberately lacks Debug: the transport may contain private file contents.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerRequest {
    pub version: u16,
    pub scope: BrokerScope,
    pub sequence: u64,
    pub operation: BrokerOperation,
}

/// Evidence to commit before invoking the synchronous filesystem port. A digest
/// is not a persistence acknowledgement; the trusted host must implement admit.
pub struct BrokerIntent {
    pub scope: BrokerScope,
    pub sequence: u64,
    pub request_sha256: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum BrokerError {
    #[error("invalid broker request")]
    Invalid,
    #[error("broker scope or sequence refused")]
    Refused,
    #[error("broker capability revoked or expired")]
    Revoked,
    #[error("broker clock unavailable")]
    Clock,
    #[error("broker intent admission failed; capability closed")]
    Admission,
    #[error("broker operation outcome uncertain; capability closed")]
    Uncertain,
}

fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn identity(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}
fn relative_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 4096
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
        && path
            .split('/')
            .all(|p| !matches!(p, "" | "." | ".." | ".git"))
}
impl BrokerScope {
    fn valid(&self) -> bool {
        digest(&self.authority_id)
            && self.boot_generation > 0
            && self.lease_epoch > 0
            && identity(&self.task_id)
            && identity(&self.attempt_id)
            && uuid::Uuid::parse_str(&self.invocation_id)
                .is_ok_and(|u| !u.is_nil() && u.to_string() == self.invocation_id)
    }
}
impl BrokerRequest {
    /// Decode bounded, strict protocol input. Physical path checks remain required.
    /// # Errors
    /// Refuses unsupported versions/operations, identities, paths and payload bounds.
    pub fn decode(bytes: &[u8]) -> Result<Self, BrokerError> {
        if bytes.len() > MAX_BROKER_FRAME {
            return Err(BrokerError::Invalid);
        }
        let request: Self = serde_json::from_slice(bytes).map_err(|_| BrokerError::Invalid)?;
        request.validate()?;
        Ok(request)
    }
    fn validate(&self) -> Result<(), BrokerError> {
        if self.version != 1
            || !self.scope.valid()
            || !(1..=MAX_OPERATIONS).contains(&self.sequence)
        {
            return Err(BrokerError::Invalid);
        }
        let valid = match &self.operation {
            BrokerOperation::ReadText { path } => relative_path(path),
            BrokerOperation::WriteText {
                path,
                expected_sha256,
                contents,
            } => {
                relative_path(path)
                    && contents.len() <= MAX_TEXT
                    && expected_sha256.as_ref().is_none_or(|h| digest(h))
            }
        };
        if valid {
            Ok(())
        } else {
            Err(BrokerError::Invalid)
        }
    }
    fn intent(&self) -> Result<BrokerIntent, BrokerError> {
        self.validate()?;
        let encoded = serde_json::to_vec(self).map_err(|_| BrokerError::Invalid)?;
        if encoded.len() > MAX_BROKER_FRAME {
            return Err(BrokerError::Invalid);
        }
        Ok(BrokerIntent {
            scope: self.scope.clone(),
            sequence: self.sequence,
            request_sha256: format!("{:x}", Sha256::digest(encoded)),
        })
    }
}

/// Serial capability gate. No method reopens it. Only the wrapper owns this
/// object; the MCP proxy supplies requests, never callbacks or a trusted scope.
/// Ports must complete synchronously without retaining a write capability or
/// spawning deferred work. This primitive alone is not a persisted write fence.
pub struct FileBrokerGate {
    scope: BrokerScope,
    deadline_ms: u64,
    closed: AtomicBool,
    uncertain: AtomicBool,
    completed: Mutex<u64>,
}
impl FileBrokerGate {
    /// Construct from trusted, already-authorized launch state and a host deadline.
    /// # Errors
    /// Rejects malformed scope or an expired/unavailable continuous clock.
    pub fn new(scope: BrokerScope, deadline_ms: u64) -> Result<Self, BrokerError> {
        if !scope.valid() {
            return Err(BrokerError::Invalid);
        }
        if crate::continuous_millis().map_err(|_| BrokerError::Clock)? >= deadline_ms {
            return Err(BrokerError::Revoked);
        }
        Ok(Self {
            scope,
            deadline_ms,
            closed: AtomicBool::new(false),
            uncertain: AtomicBool::new(false),
            completed: Mutex::new(0),
        })
    }
    fn check_open(&self) -> Result<(), BrokerError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(BrokerError::Revoked);
        }
        let now = crate::continuous_millis().map_err(|_| {
            self.closed.store(true, Ordering::SeqCst);
            BrokerError::Clock
        })?;
        if now >= self.deadline_ms {
            self.closed.store(true, Ordering::SeqCst);
            return Err(BrokerError::Revoked);
        }
        Ok(())
    }
    /// Commit the exact intent, then run one synchronous operation while holding
    /// the gate. Recheck revocation after admission: a committed but unexecuted
    /// intent remains the caller's recovery responsibility. No replay is granted.
    /// # Errors
    /// Rejects stale/replayed input; failed ports close the gate. A panic poisons
    /// it, so neither further execution nor a clean revocation result is possible.
    pub fn execute<T>(
        &self,
        request: &BrokerRequest,
        admit: impl FnOnce(&BrokerIntent) -> Result<(), BrokerError>,
        apply: impl FnOnce(&BrokerRequest) -> Result<T, BrokerError>,
    ) -> Result<T, BrokerError> {
        self.execute_recorded(request, admit, apply, |_, _| Ok(()))
    }
    /// Persist successful results under the same serialization lock before
    /// returning or granting a clean revocation acknowledgement.
    /// # Errors
    /// Failed result persistence closes the capability and retains uncertainty,
    /// even if the filesystem effect itself completed successfully.
    pub fn execute_recorded<T>(
        &self,
        request: &BrokerRequest,
        admit: impl FnOnce(&BrokerIntent) -> Result<(), BrokerError>,
        apply: impl FnOnce(&BrokerRequest) -> Result<T, BrokerError>,
        complete: impl FnOnce(&BrokerIntent, &T) -> Result<(), BrokerError>,
    ) -> Result<T, BrokerError> {
        let intent = request.intent()?;
        self.check_open()?;
        let mut completed = self.completed.lock().map_err(|_| BrokerError::Uncertain)?;
        self.check_open()?;
        if request.scope != self.scope || request.sequence != *completed + 1 {
            return Err(BrokerError::Refused);
        }
        self.uncertain.store(true, Ordering::SeqCst);
        if admit(&intent).is_err() {
            self.closed.store(true, Ordering::SeqCst);
            return Err(BrokerError::Admission);
        }
        self.check_open()?;
        if let Ok(value) = apply(request) {
            if complete(&intent, &value).is_err() {
                self.closed.store(true, Ordering::SeqCst);
                return Err(BrokerError::Uncertain);
            }
            *completed = request.sequence;
            self.uncertain.store(false, Ordering::SeqCst);
            Ok(value)
        } else {
            self.closed.store(true, Ordering::SeqCst);
            Err(BrokerError::Uncertain)
        }
    }
    /// Stop admitting work without waiting for an in-flight synchronous port.
    /// This is a cancellation request, not acknowledgement that writes stopped.
    pub fn request_revocation(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
    /// Close admission immediately, then wait for any synchronous port to finish.
    /// Returns only the last completed sequence, not a durable receipt or proof
    /// that a failed admitted intent did not change the filesystem.
    /// # Errors
    /// A failed, partially admitted or poisoned operation remains uncertain; no
    /// successful fence is returned.
    pub fn revoke(&self) -> Result<u64, BrokerError> {
        self.request_revocation();
        let completed = self.completed.lock().map_err(|_| BrokerError::Uncertain)?;
        if self.uncertain.load(Ordering::SeqCst) {
            return Err(BrokerError::Uncertain);
        }
        Ok(*completed)
    }
}
