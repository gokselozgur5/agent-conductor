//! Parent-owned broker process transport. Native admission and durable owner
//! persistence remain caller responsibilities; dropping closes the control pipe.
use crate::{
    BrokerError, BrokerReceiptDirectory, BrokerRunnerControl, BrokerRunnerEvent,
    BrokerRunnerLaunch, BrokerShutdownObservation, BrokerShutdownReceipt, ChildIdentity,
    broker_root_identity,
};
use std::{
    future::Future,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

pub struct PreparedBrokerRunner {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    frame: Vec<u8>,
    expected: BrokerShutdownReceipt,
    receipt_root: PathBuf,
    receipt_root_sha256: String,
    task_root: PathBuf,
    maximum_deadline_ms: u64,
    proxy_handoff: String,
}
/// Released only after the persistence callback succeeds and identities are
/// rechecked. No destructor invents a receipt, database closure or process fence.
pub struct BrokerRunnerSession(PreparedBrokerRunner);
impl PreparedBrokerRunner {
    /// Spawn an already approved/pinned wrapper and observe Ready without Release.
    /// The caller must durably prepare this exact launch before calling; neither
    /// this method nor a file hash certifies arbitrary executable behavior.
    /// # Errors
    /// Rejects invalid launch, missing private receipt root, bad Ready or identity.
    pub async fn prepare(wrapper: &Path, launch: &BrokerRunnerLaunch) -> Result<Self, BrokerError> {
        let hash = launch.sha256()?;
        if !wrapper.is_absolute() {
            return Err(BrokerError::Invalid);
        }
        BrokerReceiptDirectory::open(&launch.receipt_root)?;
        let receipt_root_sha256 = broker_root_identity(&launch.receipt_root)?;
        let mut child = Command::new(wrapper)
            .arg("--file-broker")
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(false)
            .spawn()
            .map_err(|_| BrokerError::Uncertain)?;
        let mut input = child.stdin.take().ok_or(BrokerError::Uncertain)?;
        let output = BufReader::new(child.stdout.take().ok_or(BrokerError::Uncertain)?);
        send_frame(&mut input, launch).await?;
        let process = ChildIdentity::observe(child.id().ok_or(BrokerError::Uncertain)?)
            .await
            .map_err(|_| BrokerError::Refused)?;
        let mut prepared = Self {
            child,
            input,
            output,
            frame: Vec::new(),
            expected: BrokerShutdownReceipt {
                version: 1,
                scope: launch.scope.clone(),
                launch_sha256: hash.clone(),
                root_identity_sha256: launch.root_identity_sha256.clone(),
                process: process.clone(),
                observation: BrokerShutdownObservation::NeverReleased,
            },
            receipt_root: launch.receipt_root.clone(),
            receipt_root_sha256,
            task_root: launch.root.clone(),
            maximum_deadline_ms: launch.maximum_deadline_ms,
            proxy_handoff: String::new(),
        };
        match tokio::time::timeout(Duration::from_secs(10), prepared.event())
            .await
            .map_err(|_| BrokerError::Uncertain)??
        {
            BrokerRunnerEvent::Ready {
                launch_sha256,
                root_identity_sha256,
                proxy_handoff,
                process: claimed,
            } if launch_sha256 == hash
                && root_identity_sha256 == launch.root_identity_sha256
                && claimed == process =>
            {
                prepared.proxy_handoff = proxy_handoff;
            }
            _ => return Err(BrokerError::Refused),
        }
        prepared.revalidate().await?;
        Ok(prepared)
    }
    /// Public metadata suitable for durable owner records. Contains no handoff.
    #[must_use]
    pub fn identity(&self) -> &BrokerShutdownReceipt {
        &self.expected
    }
    async fn revalidate(&self) -> Result<(), BrokerError> {
        BrokerReceiptDirectory::open(&self.receipt_root)?;
        self.expected
            .process
            .revalidate()
            .await
            .map_err(|_| BrokerError::Refused)?;
        if broker_root_identity(&self.task_root)? != self.expected.root_identity_sha256
            || broker_root_identity(&self.receipt_root)? != self.receipt_root_sha256
        {
            return Err(BrokerError::Refused);
        }
        Ok(())
    }
    /// Commit independently observed identity before Release. The callback must
    /// enforce fresh ownership, recheck the live lease and return its deadline.
    /// Cancellation/failed persistence closes stdin; no proxy handoff is exposed.
    /// # Errors
    /// Refuses failed persistence, changed roots/process or expired deadlines.
    pub async fn release<F, Fut>(mut self, record: F) -> Result<BrokerRunnerSession, BrokerError>
    where
        F: FnOnce(BrokerShutdownReceipt) -> Fut,
        Fut: Future<Output = Result<u64, BrokerError>>,
    {
        let deadline = record(self.expected.clone()).await?;
        self.revalidate().await?;
        let now = crate::continuous_millis().map_err(|_| BrokerError::Clock)?;
        if deadline <= now || deadline > self.maximum_deadline_ms {
            return Err(BrokerError::Revoked);
        }
        self.send(&BrokerRunnerControl::Release {
            launch_sha256: self.expected.launch_sha256.clone(),
            lease_deadline_ms: deadline,
        })
        .await?;
        Ok(BrokerRunnerSession(self))
    }
    async fn send(&mut self, value: &impl serde::Serialize) -> Result<(), BrokerError> {
        send_frame(&mut self.input, value).await
    }

    async fn event(&mut self) -> Result<BrokerRunnerEvent, BrokerError> {
        // Retain a partial frame if the caller cancels its wait.
        (&mut self.output)
            .take(16385_u64.saturating_sub(self.frame.len() as u64))
            .read_until(b'\n', &mut self.frame)
            .await
            .map_err(|_| BrokerError::Uncertain)?;
        if self.frame.len() > 16384 || self.frame.last() != Some(&b'\n') {
            return Err(BrokerError::Invalid);
        }
        let event = serde_json::from_slice(&self.frame).map_err(|_| BrokerError::Invalid)?;
        self.frame.clear();
        Ok(event)
    }
}
impl BrokerRunnerSession {
    /// Pass only to the explicitly approved proxy child environment, never logs,
    /// model context, command arguments or durable launch/configuration records.
    #[must_use]
    pub fn proxy_handoff(&self) -> &str {
        &self.0.proxy_handoff
    }
    /// # Errors
    /// Refuses expired/rebound identity or a lost control pipe.
    pub async fn renew(&mut self, deadline: u64) -> Result<(), BrokerError> {
        self.0.revalidate().await?;
        if deadline <= crate::continuous_millis().map_err(|_| BrokerError::Clock)?
            || deadline > self.0.maximum_deadline_ms
        {
            return Err(BrokerError::Revoked);
        }
        self.0
            .send(&BrokerRunnerControl::Renew {
                launch_sha256: self.0.expected.launch_sha256.clone(),
                lease_deadline_ms: deadline,
            })
            .await
    }
    /// Requests cancellation, not successful settlement.
    /// # Errors
    /// A lost pipe remains unknown until independent evidence is verified.
    pub async fn cancel(&mut self) -> Result<(), BrokerError> {
        self.0.send(&BrokerRunnerControl::Cancel).await
    }
    /// Wait for a complete local watermark and verify the independently persisted
    /// receipt against the parent-observed identity. Still not task acceptance or
    /// automatic store closure; the caller must also match durable operation state.
    /// # Errors
    /// Refuses unknown outcomes, failed exit, missing/mutated receipt or root change.
    pub async fn finish(mut self) -> Result<BrokerShutdownReceipt, BrokerError> {
        let event = tokio::time::timeout(Duration::from_secs(10), self.0.event())
            .await
            .map_err(|_| BrokerError::Uncertain)??;
        let BrokerRunnerEvent::Stopped {
            launch_sha256,
            completed_sequence: Some(completed_sequence),
        } = event
        else {
            return Err(BrokerError::Uncertain);
        };
        if launch_sha256 != self.0.expected.launch_sha256 {
            return Err(BrokerError::Refused);
        }
        let status = tokio::time::timeout(Duration::from_secs(5), self.0.child.wait())
            .await
            .map_err(|_| BrokerError::Uncertain)?
            .map_err(|_| BrokerError::Uncertain)?;
        if !status.success()
            || broker_root_identity(&self.0.receipt_root)? != self.0.receipt_root_sha256
        {
            return Err(BrokerError::Refused);
        }
        self.0.expected.observation = BrokerShutdownObservation::Revoked { completed_sequence };
        BrokerReceiptDirectory::open(&self.0.receipt_root)?.read(&self.0.expected)
    }
}

async fn send_frame(
    input: &mut ChildStdin,
    value: &impl serde::Serialize,
) -> Result<(), BrokerError> {
    let mut bytes = serde_json::to_vec(value).map_err(|_| BrokerError::Invalid)?;
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(2), input.write_all(&bytes))
        .await
        .map_err(|_| BrokerError::Uncertain)?
        .map_err(|_| BrokerError::Uncertain)
}
