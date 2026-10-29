//! Service ownership of a released runner and its durable journal transport.
use crate::{AcknowledgedBroker, BrokerJournalSession, RegisteredBrokerJournal};
use agent_conductor_store::{EffectAudit, FileBrokerBinding, Store};
use conductor_runtime::{BrokerError, BrokerRunnerSession, BrokerShutdownReceipt};
use std::{path::PathBuf, time::Duration};

/// Owns both control and persistence paths. Dropping closes the runner control
/// pipe and aborts its journal task, retaining unresolved durable ownership.
#[must_use]
pub struct NativeBrokerSession {
    runner: BrokerRunnerSession,
    journal: BrokerJournalSession,
    store: Store,
    binding: FileBrokerBinding,
    receipt_root: PathBuf,
}

impl AcknowledgedBroker {
    /// Register and own the journal, then persist exact process ownership before
    /// Release. The deadline must come from the caller's current lease authority.
    /// No proxy handoff is exposed until the durable release gate succeeds.
    /// # Errors
    /// Refuses stale binding, registration replay, failed owner persistence or
    /// runner revalidation. Failure retains durable uncertainty for recovery.
    pub async fn release(
        self,
        store: Store,
        binding: FileBrokerBinding,
        deadline: u64,
        journal_wait: Duration,
        event: EffectAudit<'_>,
    ) -> Result<NativeBrokerSession, BrokerError> {
        let launch = store
            .file_broker_launch(&binding)
            .await
            .map_err(|_| BrokerError::Refused)?;
        let receipt_root = PathBuf::from(launch.receipt_root);
        let journal = RegisteredBrokerJournal::register(store.clone(), binding.clone())
            .await?
            .start_remote(self.journal_stream, journal_wait)?;
        let (store_ref, binding_ref, root_ref) = (&store, &binding, &receipt_root);
        let runner = self
            .runner
            .release(|identity| async move {
                crate::record_broker_owner(store_ref, binding_ref, &identity, root_ref, event)
                    .await?;
                Ok(deadline)
            })
            .await?;
        Ok(NativeBrokerSession {
            runner,
            journal,
            store,
            binding,
            receipt_root,
        })
    }
}

impl NativeBrokerSession {
    /// Give only to the approved isolated proxy child environment, never logs.
    #[must_use]
    pub fn proxy_handoff(&self) -> &str {
        self.runner.proxy_handoff()
    }

    /// # Errors
    /// Refuses expired identity/deadline or a lost runner control pipe.
    pub async fn renew(&mut self, deadline: u64) -> Result<(), BrokerError> {
        self.runner.renew(deadline).await
    }

    /// Request revocation; this is not durable settlement or execution fencing.
    /// # Errors
    /// A lost control pipe remains unresolved until independently recovered.
    pub async fn cancel(&mut self) -> Result<(), BrokerError> {
        self.runner.cancel().await
    }

    /// Verify runner shutdown, observe journal completion, then atomically close
    /// durable broker ownership. Never fences execution or accepts a task.
    /// Cancelling this future drops both transport owners without claiming closure.
    /// # Errors
    /// Refuses uncertain shutdown, journal failure or conflicting durable evidence.
    pub async fn finish(
        self,
        event: EffectAudit<'_>,
    ) -> Result<BrokerShutdownReceipt, BrokerError> {
        let receipt = self.runner.finish().await?;
        self.journal.finish().await?;
        crate::close_broker_from_receipt(
            &self.store,
            &self.binding,
            &receipt,
            &self.receipt_root,
            event,
        )
        .await?;
        Ok(receipt)
    }
}
