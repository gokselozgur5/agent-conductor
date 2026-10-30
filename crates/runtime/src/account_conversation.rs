//! Conversation protocol over the existing kernel-UID-authenticated account channel.
//! The coordinator must persist one-shot launch intent before connecting, then
//! persist the exact Ready binding before Release. No reconnect, retry, task
//! verdict or unrestricted descendant fence is implied. This reusable API is not
//! yet selected by the persistent account-worker dispatcher or conversation service.
use crate::{AccountChannelAddress, ExecutionAccount, VendorPin};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{future::Future, path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
};

const FRAME_LIMIT: usize = 512 * 1024;
const HANDSHAKE: Duration = Duration::from_secs(5);
const VERSION: u32 = 1;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountConversationRequest {
    pub version: u32,
    pub launch_id: String,
    pub account: ExecutionAccount,
    pub vendor: VendorPin,
    pub directory: PathBuf,
    pub path: String,
    pub prompt: String,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountConversationReady {
    pub launch_id: String,
    pub request_sha256: String,
    pub worker_uid: u32,
    pub coordinator_uid: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AccountConversationEvent {
    Ready {
        binding: AccountConversationReady,
    },
    Finished {
        binding: AccountConversationReady,
        text: String,
        output_sha256: String,
    },
    /// No success or fencing claim. Reconciliation is required before any retry.
    Unknown {
        binding: AccountConversationReady,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Control {
    Release { binding: AccountConversationReady },
    Cancel,
}

#[derive(Debug, thiserror::Error)]
pub enum AccountConversationError {
    #[error("account conversation request or binding was refused")]
    Refused,
    #[error("account conversation framing or protocol failed")]
    Protocol,
    #[error("account conversation transport failed; no automatic retry is permitted")]
    Transport,
    #[error("account conversation durable acknowledgement failed")]
    Persistence,
}

impl AccountConversationRequest {
    /// Hash the bounded immutable request; this does not validate worker identity
    /// on the coordinator or read any authentication files.
    /// # Errors
    /// Refuses invalid IDs, bounds, paths or unsupported protocol versions.
    pub fn sha256(&self) -> Result<String, AccountConversationError> {
        if self.version != VERSION
            || self.launch_id.is_empty()
            || self.launch_id.len() > 128
            || !self
                .launch_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b':'))
            || self.prompt.trim().is_empty()
            || self.prompt.len() > 60_000
            || self.timeout_ms == 0
            || self.timeout_ms > 300_000
            || !self.directory.is_absolute()
            || self.path.is_empty()
            || self.path.len() > 16_384
        {
            return Err(AccountConversationError::Refused);
        }
        let bytes = serde_json::to_vec(self).map_err(|_| AccountConversationError::Protocol)?;
        if bytes.len() >= FRAME_LIMIT {
            return Err(AccountConversationError::Refused);
        }
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Clone, Copy)]
enum Phase {
    AwaitingReady,
    AwaitingTerminal,
    Terminal,
}

pub struct AccountConversationConnection {
    input: OwnedWriteHalf,
    output: BufReader<OwnedReadHalf>,
    binding: AccountConversationReady,
    frame: Vec<u8>,
    deadline: tokio::time::Instant,
    phase: Phase,
}

impl AccountConversationConnection {
    /// Authenticates both selected UIDs and request digest, receives validated
    /// Ready, then awaits durable recording of that exact binding before Release.
    /// Caller must already own a persisted one-shot native launch reservation.
    /// # Errors
    /// Refuses changed bindings, failed persistence or transport/protocol errors.
    /// Errors never permit an automatic retry of a reserved native turn.
    pub async fn connect<F, Fut>(
        address: &AccountChannelAddress,
        request: &AccountConversationRequest,
        record_ack: F,
    ) -> Result<Self, AccountConversationError>
    where
        F: FnOnce(AccountConversationReady) -> Fut,
        Fut: Future<Output = Result<(), AccountConversationError>>,
    {
        let digest = request.sha256()?;
        if digest != address.packet_sha256 || request.account.uid != address.worker_uid {
            return Err(AccountConversationError::Refused);
        }
        let stream = crate::connect_account_channel(address, HANDSHAKE)
            .await
            .map_err(|_| AccountConversationError::Transport)?;
        let (output, mut input) = stream.into_split();
        send(&mut input, request).await?;
        let expected = AccountConversationReady {
            launch_id: request.launch_id.clone(),
            request_sha256: digest,
            worker_uid: address.worker_uid,
            coordinator_uid: address.coordinator_uid,
        };
        let mut connection = Self {
            input,
            output: BufReader::new(output),
            binding: expected,
            frame: Vec::new(),
            deadline: tokio::time::Instant::now() + HANDSHAKE,
            phase: Phase::AwaitingReady,
        };
        let event = tokio::time::timeout(HANDSHAKE, connection.event())
            .await
            .map_err(|_| AccountConversationError::Transport)??;
        let AccountConversationEvent::Ready { binding } = event else {
            return Err(AccountConversationError::Protocol);
        };
        tokio::time::timeout(HANDSHAKE, record_ack(binding))
            .await
            .map_err(|_| AccountConversationError::Persistence)??;
        connection.deadline =
            tokio::time::Instant::now() + Duration::from_millis(request.timeout_ms) + HANDSHAKE;
        send(
            &mut connection.input,
            &Control::Release {
                binding: connection.binding.clone(),
            },
        )
        .await?;
        Ok(connection)
    }

    /// Reads a bounded frame, retaining partial bytes if the caller cancels this
    /// read. Finished is parsed output only, never a managed-task acceptance.
    /// # Errors
    /// Invalid bindings and disconnects retain uncertainty; never reconnect here.
    pub async fn event(&mut self) -> Result<AccountConversationEvent, AccountConversationError> {
        if matches!(self.phase, Phase::Terminal) {
            return Err(AccountConversationError::Protocol);
        }
        tokio::time::timeout_at(self.deadline, read_frame(&mut self.output, &mut self.frame))
            .await
            .map_err(|_| AccountConversationError::Transport)??;
        let phase = self.phase;
        // Poison malformed complete events, but retain phase and partial bytes
        // if the caller cancels this future while reading.
        self.phase = Phase::Terminal;
        let event: AccountConversationEvent =
            serde_json::from_slice(&self.frame).map_err(|_| AccountConversationError::Protocol)?;
        self.frame.clear();
        let actual = match &event {
            AccountConversationEvent::Ready { binding }
            | AccountConversationEvent::Unknown { binding }
            | AccountConversationEvent::Finished { binding, .. } => binding,
        };
        if actual != &self.binding {
            return Err(AccountConversationError::Refused);
        }
        if let AccountConversationEvent::Finished {
            text,
            output_sha256,
            ..
        } = &event
            && (text.trim().is_empty() || text.len() > 65_536 || !valid_digest(output_sha256))
        {
            return Err(AccountConversationError::Protocol);
        }
        match (&event, phase) {
            (AccountConversationEvent::Ready { .. }, Phase::AwaitingReady) => {
                self.phase = Phase::AwaitingTerminal;
            }
            (
                AccountConversationEvent::Finished { .. }
                | AccountConversationEvent::Unknown { .. },
                Phase::AwaitingTerminal,
            ) => {}
            _ => return Err(AccountConversationError::Protocol),
        }
        Ok(event)
    }

    /// Requests cancellation; the caller must still await terminal supervision.
    /// Sending or receiving cancellation does not prove arbitrary descendants fenced.
    /// # Errors
    /// Transport failure leaves the recorded turn uncertain.
    pub async fn cancel(&mut self) -> Result<(), AccountConversationError> {
        if !matches!(self.phase, Phase::AwaitingTerminal) {
            return Err(AccountConversationError::Protocol);
        }
        send(&mut self.input, &Control::Cancel).await
    }
}

/// Serves one request on an already UID-authenticated account-channel stream.
/// The caller supplies the admitted digest, not a digest taken from model output.
/// Preparation validates the receiver's own account and pin before Ready. Native
/// execution begins only after a matching Release. Disconnect/cancel awaits the
/// owned process future; no supervisor future is detached.
/// # Errors
/// Refuses invalid peer/request/account/pin/release; transport failure is uncertain.
/// Callers must retain this future until it finishes, including during shutdown.
pub async fn serve_account_conversation(
    stream: UnixStream,
    request: AccountConversationRequest,
    admitted_digest: &str,
) -> Result<(), AccountConversationError> {
    let digest = request.sha256()?;
    if digest != admitted_digest {
        return Err(AccountConversationError::Refused);
    }
    let coordinator_uid = stream
        .peer_cred()
        .map_err(|_| AccountConversationError::Transport)?
        .uid();
    let spec = crate::full_capability_command(
        &request.account,
        &request.vendor,
        request.directory.clone(),
        &request.path,
        request.prompt.clone(),
        Duration::from_millis(request.timeout_ms),
    )
    .map_err(|_| AccountConversationError::Refused)?;
    let binding = AccountConversationReady {
        launch_id: request.launch_id,
        request_sha256: digest,
        worker_uid: request.account.uid,
        coordinator_uid,
    };
    let (input, mut output) = stream.into_split();
    let mut input = BufReader::new(input);
    send(
        &mut output,
        &AccountConversationEvent::Ready {
            binding: binding.clone(),
        },
    )
    .await?;
    let release = tokio::time::timeout(HANDSHAKE, read_control(&mut input))
        .await
        .map_err(|_| AccountConversationError::Transport)??;
    if !matches!(release, Control::Release { binding: ref received } if received == &binding) {
        return Err(AccountConversationError::Refused);
    }
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let process = crate::run_process(&spec, stopped);
    tokio::pin!(process);
    let result = tokio::select! {
        output = &mut process => output,
        _ = read_control(&mut input) => {
            let _ = stop.send(true);
            process.await
        }
    };
    let reply = result
        .ok()
        .and_then(|output| crate::interpret_conversation_output(request.vendor.kind, &output).ok());
    let event = match reply {
        Some(reply) if !reply.text.trim().is_empty() && reply.text.len() <= 65_536 => {
            AccountConversationEvent::Finished {
                binding,
                text: reply.text,
                output_sha256: reply.output_sha256,
            }
        }
        _ => AccountConversationEvent::Unknown { binding },
    };
    send(&mut output, &event).await
}

async fn read_control(
    input: &mut (impl AsyncBufRead + Unpin),
) -> Result<Control, AccountConversationError> {
    let mut frame = Vec::new();
    read_frame(input, &mut frame).await?;
    serde_json::from_slice(&frame).map_err(|_| AccountConversationError::Protocol)
}

async fn read_frame(
    input: &mut (impl AsyncBufRead + Unpin),
    frame: &mut Vec<u8>,
) -> Result<(), AccountConversationError> {
    let remaining = FRAME_LIMIT
        .checked_sub(frame.len())
        .ok_or(AccountConversationError::Protocol)?;
    let count = input
        .take(remaining as u64 + 1)
        .read_until(b'\n', frame)
        .await
        .map_err(|_| AccountConversationError::Transport)?;
    if count == 0 || frame.len() > FRAME_LIMIT || frame.last() != Some(&b'\n') {
        return Err(AccountConversationError::Protocol);
    }
    Ok(())
}

async fn send(
    output: &mut (impl AsyncWrite + Unpin),
    value: &impl Serialize,
) -> Result<(), AccountConversationError> {
    let mut frame = serde_json::to_vec(value).map_err(|_| AccountConversationError::Protocol)?;
    frame.push(b'\n');
    if frame.len() > FRAME_LIMIT {
        return Err(AccountConversationError::Protocol);
    }
    tokio::time::timeout(HANDSHAKE, output.write_all(&frame))
        .await
        .map_err(|_| AccountConversationError::Transport)?
        .map_err(|_| AccountConversationError::Transport)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
