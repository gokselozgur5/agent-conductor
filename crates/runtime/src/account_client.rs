//! Controller protocol over an authenticated account channel. This connection
//! does not own a child process or turn peer-reported data into process authority.
use crate::{
    AccountChannelAddress, AccountNativeLaunch, PacketAcknowledgement,
    RunnerConnectionError as Error, RunnerControl, RunnerEvent,
};
use std::{future::Future, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::unix::{OwnedReadHalf, OwnedWriteHalf},
};

pub struct AccountRunnerConnection {
    input: OwnedWriteHalf,
    output: BufReader<OwnedReadHalf>,
    packet_sha256: String,
    frame: Vec<u8>,
}

impl AccountRunnerConnection {
    /// Verify account/packet binding, then await the caller's durable ACK and
    /// current lease before Release. The callback must enforce current ownership.
    /// No child identity is supplied: the account receiver is not our owned child.
    /// # Errors
    /// Refuses mismatched contracts, failed admission, ACK/persistence or expiry.
    pub async fn connect<F, Fut>(
        address: &AccountChannelAddress,
        launch: &AccountNativeLaunch,
        record_ack: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(PacketAcknowledgement) -> Fut,
        Fut: Future<Output = Result<u64, Error>>,
    {
        launch.packet.validate().map_err(|_| Error::Protocol)?;
        let digest = launch.packet.sha256().map_err(|_| Error::Protocol)?;
        let account = &launch
            .packet
            .contract
            .full_capability
            .as_ref()
            .ok_or(Error::Protocol)?
            .account;
        if digest != address.packet_sha256 || account.uid != address.worker_uid {
            return Err(Error::Protocol);
        }
        let stream = crate::connect_account_channel(address, Duration::from_secs(5))
            .await
            .map_err(|_| Error::Transport)?;
        let (output, input) = stream.into_split();
        let mut connection = Self {
            input,
            output: BufReader::new(output),
            packet_sha256: digest,
            frame: Vec::new(),
        };
        connection.send(launch).await?;
        let RunnerEvent::Ready { acknowledgement } =
            tokio::time::timeout(Duration::from_secs(5), connection.event())
                .await
                .map_err(|_| Error::Transport)??
        else {
            return Err(Error::Protocol);
        };
        launch
            .packet
            .verify_ack(&acknowledgement)
            .map_err(|_| Error::Protocol)?;
        let deadline = tokio::time::timeout(Duration::from_secs(3), record_ack(acknowledgement))
            .await
            .map_err(|_| Error::Persistence)??;
        if crate::continuous_millis().map_err(|_| Error::Protocol)? >= deadline {
            return Err(Error::Persistence);
        }
        connection
            .send(&RunnerControl::Release {
                packet_sha256: connection.packet_sha256.clone(),
                continuous_deadline_ms: deadline,
            })
            .await?;
        Ok(connection)
    }

    /// Renew only after the caller validates the current owner and lease.
    /// # Errors
    /// Fails on transport loss; no implicit reconnect or attempt replay occurs.
    pub async fn renew(&mut self, continuous_deadline_ms: u64) -> Result<(), Error> {
        self.send(&RunnerControl::Renew {
            packet_sha256: self.packet_sha256.clone(),
            continuous_deadline_ms,
        })
        .await
    }

    /// Ask the receiver to stop its supervised group; this does not prove fencing.
    /// # Errors
    /// Fails on transport loss. Silence is not a stop acknowledgement.
    pub async fn cancel(&mut self) -> Result<(), Error> {
        self.send(&RunnerControl::Cancel).await
    }

    /// Read a bounded receiver event, retaining partial bytes across cancellation.
    /// Finished/Stopped are scoped observations, not receiver exit or PID authority.
    /// # Errors
    /// Malformed/truncated/oversized frames or transport loss require reconciliation.
    pub async fn event(&mut self) -> Result<RunnerEvent, Error> {
        const LIMIT: usize = 9 * 1024 * 1024;
        let remaining = LIMIT.checked_sub(self.frame.len()).ok_or(Error::Protocol)?;
        let count = (&mut self.output)
            .take(remaining as u64 + 1)
            .read_until(b'\n', &mut self.frame)
            .await
            .map_err(|_| Error::Transport)?;
        if count == 0 || self.frame.len() > LIMIT || self.frame.last() != Some(&b'\n') {
            return Err(Error::Protocol);
        }
        let event = serde_json::from_slice(&self.frame).map_err(|_| Error::Protocol)?;
        self.frame.clear();
        Ok(event)
    }

    async fn send(&mut self, value: &impl serde::Serialize) -> Result<(), Error> {
        let mut bytes = serde_json::to_vec(value).map_err(|_| Error::Protocol)?;
        bytes.push(b'\n');
        tokio::time::timeout(Duration::from_secs(2), self.input.write_all(&bytes))
            .await
            .map_err(|_| Error::Transport)?
            .map_err(|_| Error::Transport)
    }
}
