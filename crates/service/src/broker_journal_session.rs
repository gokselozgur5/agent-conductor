//! Owned journal task: dropping its owner never detaches a persistence path.
use crate::RegisteredBrokerJournal;
use conductor_runtime::BrokerError;
use std::time::Duration;
use tokio::{net::UnixStream, sync::watch, task::JoinHandle};

/// Owns the remote journal server until observed completion or cancellation.
/// Dropping cancels the task; durable registration and pending effects stay open.
/// A cancelled `SQLx` transaction rolls back, but already committed evidence remains.
#[must_use]
pub struct BrokerJournalSession {
    task: JoinHandle<Result<(), BrokerError>>,
    _hold: watch::Sender<bool>,
}

impl RegisteredBrokerJournal {
    /// Start the journal with an abort-on-drop owner before releasing its runner.
    /// No handoff or release authorization is granted by this operation.
    /// # Errors
    /// Rejects invalid deadlines or an unavailable Tokio runtime before spawning.
    pub fn start_remote(
        self,
        stream: UnixStream,
        wait: Duration,
    ) -> Result<BrokerJournalSession, BrokerError> {
        if wait.is_zero() || wait > Duration::from_secs(30) {
            return Err(BrokerError::Invalid);
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| BrokerError::Invalid)?;
        let (hold, cancel) = watch::channel(false);
        Ok(BrokerJournalSession {
            task: runtime.spawn(self.serve_remote(stream, cancel, wait)),
            _hold: hold,
        })
    }
}

impl BrokerJournalSession {
    /// Observe the journal's outcome after runner shutdown. Cancellation of this
    /// wait also drops and aborts the owned server; it never closes durable records.
    /// # Errors
    /// Reports transport, persistence, cancellation and task-join failures.
    pub async fn finish(mut self) -> Result<(), BrokerError> {
        (&mut self.task).await.map_err(|_| BrokerError::Uncertain)?
    }
}

impl Drop for BrokerJournalSession {
    fn drop(&mut self) {
        self.task.abort();
    }
}
