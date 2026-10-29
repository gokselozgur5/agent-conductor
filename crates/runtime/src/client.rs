//! Trusted controller connection to one wrapper. Dropping the connection closes
//! stdin so the independent watchdog stops the supervised group. This does not
//! prove detached descendants stopped. Never kill the wrapper first and thereby
//! disable that watchdog.
use crate::{PacketAcknowledgement, RunnerControl, RunnerEvent, RunnerLaunch};
use std::{future::Future, path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

pub struct RunnerConnection {
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    child: Child,
    packet_sha256: String,
    frame: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum RunnerConnectionError {
    #[error("runner transport failed")]
    Transport,
    #[error("runner acknowledgement or output was invalid")]
    Protocol,
    #[error("durable acknowledgement refused release")]
    Persistence,
}

impl RunnerConnection {
    /// Immutable packet identity bound to this connection's release and renewals.
    #[must_use]
    pub fn packet_sha256(&self) -> &str {
        &self.packet_sha256
    }

    /// Launches a wrapper, validates its acknowledgement, and records it through
    /// the controller callback before releasing the worker. The caller must have
    /// obtained a fresh durable preparation; replay must never call this method.
    /// The callback must recheck live lease/epoch ownership atomically and return
    /// its absolute expiry in the host suspend-aware continuous clock.
    /// # Errors
    /// Refuses failed transport, invalid acknowledgement or failed persistence.
    pub async fn launch<F, Fut>(
        executable: &Path,
        launch: &RunnerLaunch,
        identity: Option<&crate::RunnerProcessIdentity>,
        record_acknowledgement: F,
    ) -> Result<Self, RunnerConnectionError>
    where
        F: FnOnce(PacketAcknowledgement, crate::ChildIdentity) -> Fut,
        Fut: Future<Output = Result<u64, RunnerConnectionError>>,
    {
        Self::connect(
            executable,
            &launch.packet,
            launch,
            identity,
            false,
            record_acknowledgement,
        )
        .await
    }

    /// Starts the account-native entry; the receiving wrapper, not the sender,
    /// constructs the native command and validates its local OS account and pin.
    /// Durable acknowledgement still precedes worker release.
    /// # Errors
    /// Refuses invalid packets, receiver identity mismatch and failed persistence.
    pub async fn launch_account_native<F, Fut>(
        executable: &Path,
        launch: &crate::AccountNativeLaunch,
        identity: Option<&crate::RunnerProcessIdentity>,
        record_acknowledgement: F,
    ) -> Result<Self, RunnerConnectionError>
    where
        F: FnOnce(PacketAcknowledgement, crate::ChildIdentity) -> Fut,
        Fut: Future<Output = Result<u64, RunnerConnectionError>>,
    {
        Self::connect(
            executable,
            &launch.packet,
            launch,
            identity,
            true,
            record_acknowledgement,
        )
        .await
    }

    async fn connect<F, Fut>(
        executable: &Path,
        packet: &crate::ResumePacket,
        frame: &impl serde::Serialize,
        identity: Option<&crate::RunnerProcessIdentity>,
        account_native: bool,
        record_acknowledgement: F,
    ) -> Result<Self, RunnerConnectionError>
    where
        F: FnOnce(PacketAcknowledgement, crate::ChildIdentity) -> Fut,
        Fut: Future<Output = Result<u64, RunnerConnectionError>>,
    {
        packet
            .validate()
            .map_err(|_| RunnerConnectionError::Protocol)?;
        if !executable.is_absolute() {
            return Err(RunnerConnectionError::Protocol);
        }
        let packet_sha256 = packet
            .sha256()
            .map_err(|_| RunnerConnectionError::Protocol)?;
        let mut command = Command::new(executable);
        if account_native {
            command.arg("--account-native");
        }
        if let Some(identity) = identity {
            if identity.packet_sha256 != packet_sha256 {
                return Err(RunnerConnectionError::Protocol);
            }
            command.arg(
                identity
                    .argument()
                    .map_err(|_| RunnerConnectionError::Protocol)?,
            );
        }
        let mut child = command
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| RunnerConnectionError::Transport)?;
        let input = child.stdin.take().ok_or(RunnerConnectionError::Transport)?;
        let output = BufReader::new(
            child
                .stdout
                .take()
                .ok_or(RunnerConnectionError::Transport)?,
        );
        let mut connection = Self {
            input,
            output,
            child,
            packet_sha256,
            frame: Vec::new(),
        };
        connection.send(frame).await?;
        let RunnerEvent::Ready { acknowledgement } =
            tokio::time::timeout(Duration::from_secs(5), connection.event())
                .await
                .map_err(|_| RunnerConnectionError::Transport)??
        else {
            return Err(RunnerConnectionError::Protocol);
        };
        packet
            .verify_ack(&acknowledgement)
            .map_err(|_| RunnerConnectionError::Protocol)?;
        // The parent retains an unreaped child. Worker output cannot nominate this PID.
        let pid = connection
            .child
            .id()
            .ok_or(RunnerConnectionError::Protocol)?;
        let birth =
            tokio::time::timeout(Duration::from_secs(3), crate::ChildIdentity::observe(pid))
                .await
                .map_err(|_| RunnerConnectionError::Protocol)?
                .map_err(|_| RunnerConnectionError::Protocol)?;
        connection.require_unreaped_child()?;
        let continuous_deadline_ms = tokio::time::timeout(
            Duration::from_secs(3),
            record_acknowledgement(acknowledgement, birth.clone()),
        )
        .await
        .map_err(|_| RunnerConnectionError::Persistence)??;
        tokio::time::timeout(Duration::from_secs(1), birth.revalidate())
            .await
            .map_err(|_| RunnerConnectionError::Protocol)?
            .map_err(|_| RunnerConnectionError::Protocol)?;
        connection.require_unreaped_child()?;
        if crate::continuous_millis().map_err(|_| RunnerConnectionError::Protocol)?
            >= continuous_deadline_ms
        {
            return Err(RunnerConnectionError::Persistence);
        }
        connection
            .send(&RunnerControl::Release {
                packet_sha256: connection.packet_sha256.clone(),
                continuous_deadline_ms,
            })
            .await?;
        Ok(connection)
    }

    fn require_unreaped_child(&mut self) -> Result<(), RunnerConnectionError> {
        if self
            .child
            .try_wait()
            .map_err(|_| RunnerConnectionError::Transport)?
            .is_some()
        {
            return Err(RunnerConnectionError::Protocol);
        }
        Ok(())
    }

    /// Sends a renewal only after the controller has checked current ownership.
    /// # Errors
    /// Fails when the wrapper is unreachable or cannot accept bounded input.
    pub async fn renew(
        &mut self,
        continuous_deadline_ms: u64,
    ) -> Result<(), RunnerConnectionError> {
        self.send(&RunnerControl::Renew {
            packet_sha256: self.packet_sha256.clone(),
            continuous_deadline_ms,
        })
        .await
    }

    /// Requests a supervised stop; observe its scoped result and wrapper exit.
    /// # Errors
    /// Fails on transport loss; absence of acknowledgement is not proof of fencing.
    pub async fn cancel(&mut self) -> Result<(), RunnerConnectionError> {
        self.send(&RunnerControl::Cancel).await
    }

    /// Reads a bounded event. Partial bytes remain in the connection if a select
    /// cancels this future to renew the lease. The controller sets its own deadline.
    /// # Errors
    /// Returns malformed frame or transport errors. Discard the connection on error.
    pub async fn event(&mut self) -> Result<RunnerEvent, RunnerConnectionError> {
        const LIMIT: usize = 9 * 1024 * 1024;
        let remaining = LIMIT
            .checked_sub(self.frame.len())
            .ok_or(RunnerConnectionError::Protocol)?;
        let count = (&mut self.output)
            .take(remaining as u64 + 1)
            .read_until(b'\n', &mut self.frame)
            .await
            .map_err(|_| RunnerConnectionError::Transport)?;
        if count == 0 || self.frame.len() > LIMIT || self.frame.last() != Some(&b'\n') {
            return Err(RunnerConnectionError::Protocol);
        }
        let event =
            serde_json::from_slice(&self.frame).map_err(|_| RunnerConnectionError::Protocol)?;
        self.frame.clear();
        Ok(event)
    }

    /// Waits for the wrapper to exit after a terminal observation.
    /// # Errors
    /// A timeout or unsuccessful exit requires conservative recovery, not reuse.
    pub async fn wait(&mut self) -> Result<(), RunnerConnectionError> {
        let status = tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .map_err(|_| RunnerConnectionError::Transport)?
            .map_err(|_| RunnerConnectionError::Transport)?;
        if !status.success() {
            return Err(RunnerConnectionError::Protocol);
        }
        Ok(())
    }

    async fn send(&mut self, value: &impl serde::Serialize) -> Result<(), RunnerConnectionError> {
        let mut bytes = serde_json::to_vec(value).map_err(|_| RunnerConnectionError::Protocol)?;
        bytes.push(b'\n');
        tokio::time::timeout(Duration::from_secs(2), self.input.write_all(&bytes))
            .await
            .map_err(|_| RunnerConnectionError::Transport)?
            .map_err(|_| RunnerConnectionError::Transport)
    }
}
