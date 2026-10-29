//! Private wrapper-to-daemon journal channel; never a worker-facing file tool.
use crate::{BrokerError, BrokerFileResult, BrokerIntent, BrokerJournal, BrokerScope};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    net::Shutdown,
    os::unix::net::UnixStream,
    sync::Mutex,
    time::{Duration, Instant},
};

pub const MAX_JOURNAL_FRAME: usize = 4096;

/// Digests and bound identities only; file contents and paths never cross here.
/// None result means admission, Some means completion of that admitted request.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerJournalRequest {
    pub version: u16,
    pub scope: BrokerScope,
    pub sequence: u64,
    pub request_sha256: String,
    pub result_sha256: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerJournalAck {
    pub version: u16,
    /// Binds every request field, including phase, scope and result digest.
    pub journal_sha256: String,
}
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl BrokerJournalRequest {
    /// # Errors
    /// Rejects malformed, oversized, stale-version and unbounded identities.
    pub fn decode(bytes: &[u8]) -> Result<Self, BrokerError> {
        if bytes.len() > MAX_JOURNAL_FRAME {
            return Err(BrokerError::Invalid);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| BrokerError::Invalid)?;
        value.sha256()?;
        Ok(value)
    }
    /// # Errors
    /// Rejects invalid requests before any durable operation or acknowledgement.
    pub fn sha256(&self) -> Result<String, BrokerError> {
        if self.version != 1
            || !self.scope.valid()
            || !(1..=100_000).contains(&self.sequence)
            || !digest(&self.request_sha256)
            || self.result_sha256.as_ref().is_some_and(|s| !digest(s))
        {
            return Err(BrokerError::Invalid);
        }
        let bytes = serde_json::to_vec(self).map_err(|_| BrokerError::Invalid)?;
        if bytes.len() + 1 > MAX_JOURNAL_FRAME {
            return Err(BrokerError::Invalid);
        }
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}
struct Channel {
    stream: UnixStream,
    completed: u64,
    pending: Option<String>,
}
impl Drop for Channel {
    fn drop(&mut self) {
        let _ = self.stream.shutdown(Shutdown::Both);
    }
}
/// Blocking journal port for the wrapper's filesystem worker only. The trusted
/// host supplies an authenticated private connected stream, separate from the
/// runner's renewal/cancel pipe. There is no reconnect or timeout retry.
/// Transport loss after commit remains unknown even if the peer recorded it.
pub struct RemoteBrokerJournal {
    channel: Mutex<Option<Channel>>,
    scope: BrokerScope,
    wait: Duration,
}
impl RemoteBrokerJournal {
    /// # Errors
    /// Refuses invalid scope/time bounds or an unusable connected stream.
    pub fn new(
        stream: UnixStream,
        scope: BrokerScope,
        wait: Duration,
    ) -> Result<Self, BrokerError> {
        if !scope.valid() || wait.is_zero() || wait > Duration::from_secs(30) {
            return Err(BrokerError::Invalid);
        }
        stream
            .set_nonblocking(false)
            .map_err(|_| BrokerError::Refused)?;
        Ok(Self {
            channel: Mutex::new(Some(Channel {
                stream,
                completed: 0,
                pending: None,
            })),
            scope,
            wait,
        })
    }
    fn exchange(&self, intent: &BrokerIntent, result: Option<String>) -> Result<(), BrokerError> {
        let mut guard = self.channel.lock().map_err(|_| BrokerError::Uncertain)?;
        // Take ownership before validation or I/O. Every failure drops/shuts down
        // the stream; only an exact durable acknowledgement restores the channel.
        let mut channel = guard.take().ok_or(BrokerError::Revoked)?;
        if intent.scope != self.scope
            || intent.sequence != channel.completed + 1
            || match (&channel.pending, &result) {
                (None, None) => false,
                (Some(hash), Some(_)) => hash != &intent.request_sha256,
                _ => true,
            }
        {
            return Err(BrokerError::Refused);
        }
        let request = BrokerJournalRequest {
            version: 1,
            scope: intent.scope.clone(),
            sequence: intent.sequence,
            request_sha256: intent.request_sha256.clone(),
            result_sha256: result,
        };
        transact(&mut channel.stream, &request, self.wait)?;
        if request.result_sha256.is_some() {
            channel.completed = intent.sequence;
            channel.pending = None;
        } else {
            channel.pending = Some(intent.request_sha256.clone());
        }
        *guard = Some(channel);
        Ok(())
    }
}
impl BrokerJournal for RemoteBrokerJournal {
    fn admit(&self, intent: &BrokerIntent) -> Result<(), BrokerError> {
        self.exchange(intent, None)
    }
    fn complete(
        &self,
        intent: &BrokerIntent,
        result: &BrokerFileResult,
    ) -> Result<(), BrokerError> {
        let (BrokerFileResult::Text { sha256, .. } | BrokerFileResult::Written { sha256 }) = result;
        self.exchange(intent, Some(sha256.clone()))
    }
}
fn remaining(start: Instant, wait: Duration) -> Result<Duration, BrokerError> {
    wait.checked_sub(start.elapsed())
        .filter(|d| !d.is_zero())
        .ok_or(BrokerError::Uncertain)
}
fn transact(
    stream: &mut UnixStream,
    request: &BrokerJournalRequest,
    wait: Duration,
) -> Result<(), BrokerError> {
    let expected = request.sha256()?;
    let mut bytes = serde_json::to_vec(request).map_err(|_| BrokerError::Invalid)?;
    bytes.push(b'\n');
    let start = Instant::now();
    let mut sent = 0;
    while sent < bytes.len() {
        stream
            .set_write_timeout(Some(remaining(start, wait)?))
            .map_err(|_| BrokerError::Uncertain)?;
        let n = stream
            .write(&bytes[sent..])
            .map_err(|_| BrokerError::Uncertain)?;
        if n == 0 {
            return Err(BrokerError::Uncertain);
        }
        sent += n;
    }
    let mut reply = Vec::new();
    loop {
        stream
            .set_read_timeout(Some(remaining(start, wait)?))
            .map_err(|_| BrokerError::Uncertain)?;
        let mut byte = [0];
        stream
            .read_exact(&mut byte)
            .map_err(|_| BrokerError::Uncertain)?;
        reply.push(byte[0]);
        if reply.len() > 256 {
            return Err(BrokerError::Uncertain);
        }
        if byte[0] == b'\n' {
            break;
        }
    }
    remaining(start, wait)?;
    let ack: BrokerJournalAck =
        serde_json::from_slice(&reply).map_err(|_| BrokerError::Uncertain)?;
    if ack.version != 1 || ack.journal_sha256 != expected {
        return Err(BrokerError::Uncertain);
    }
    Ok(())
}
