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
    vendor_available: bool,
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
            vendor_available: true,
        })
    }
}

impl NativeBrokerSession {
    /// Give only to the approved isolated proxy child environment, never logs.
    #[must_use]
    pub fn proxy_handoff(&self) -> &str {
        self.runner.proxy_handoff()
    }

    /// Construct one vendor command from this session's exact frozen packet.
    /// The handoff enters only the returned child environment. Caller must still
    /// enforce vendor/OS admission and supervise the child with this session.
    /// # Errors
    /// Refuses changed task/attempt/root/packet, existing operations, closed broker,
    /// repeated issuance, requested cancellation or invalid vendor/proxy policy.
    pub async fn vendor_command(
        &mut self,
        prepared: conductor_runtime::PreparedBrokerVendor,
    ) -> Result<conductor_runtime::ProcessSpec, conductor_runtime::VendorError> {
        use conductor_runtime::VendorError;
        let packet = prepared.packet();
        let contract = &packet.contract;
        if !self.vendor_available
            || contract.task_id != self.binding.task_id
            || contract.attempt_id != self.binding.attempt_id
            || i64::try_from(contract.lease_epoch).ok() != Some(self.binding.lease_epoch)
            || conductor_runtime::broker_root_identity(&contract.workspace)
                .map_err(|_| VendorError::Identity)?
                != self.binding.root_identity_sha256
        {
            return Err(VendorError::Identity);
        }
        let execution = self
            .store
            .execution(&self.binding.execution_token)
            .await
            .map_err(|_| VendorError::Identity)?
            .ok_or(VendorError::Identity)?;
        if packet.sha256().map_err(|_| VendorError::Identity)?
            != execution.preparation.packet_sha256
        {
            return Err(VendorError::Identity);
        }
        let status = self
            .store
            .file_broker_status(&self.binding)
            .await
            .map_err(|_| VendorError::Identity)?;
        if status.closed || status.admitted != 0 || status.completed != 0 {
            return Err(VendorError::Identity);
        }
        let mut spec = prepared.into_spec();
        spec.environment.insert(
            conductor_runtime::BROKER_HANDOFF_ENV.into(),
            self.runner.proxy_handoff().into(),
        );
        self.vendor_available = false;
        Ok(spec)
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
        self.vendor_available = false;
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
