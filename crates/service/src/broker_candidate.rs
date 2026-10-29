//! A candidate bound inside the owned process/broker flow, never a task verdict.
use crate::{
    BrokerCandidateFailure, BrokerVendorOutcome, Clock, NativeBrokerSession,
    PersistentControlService,
};
use agent_conductor_store::{EffectAudit, ExecutionState, FileBrokerBinding, StoreError};
use conductor_runtime::{
    BrokerShutdownObservation, BrokerShutdownReceipt, PreparedBrokerVendor, ProcessOutput,
    ResumePacket, VendorCandidate, interpret_vendor_output,
};

/// Only the service-owned run can construct this observation. It establishes
/// neither native adapter admission nor an execution fence. Publication must
/// revalidate current ownership; successful completion still requires review.
pub struct BrokerCandidate {
    binding: FileBrokerBinding,
    candidate: VendorCandidate,
    output: ProcessOutput,
    receipt: BrokerShutdownReceipt,
}
impl BrokerCandidate {
    #[must_use]
    pub fn binding(&self) -> &FileBrokerBinding {
        &self.binding
    }
    #[must_use]
    pub fn candidate(&self) -> &VendorCandidate {
        &self.candidate
    }
    /// Private vendor output may contain task data; do not expose it as telemetry.
    #[must_use]
    pub fn output(&self) -> &ProcessOutput {
        &self.output
    }
    #[must_use]
    pub fn receipt(&self) -> &BrokerShutdownReceipt {
        &self.receipt
    }
}

impl<C: Clock> PersistentControlService<C> {
    /// Run and bind a successful candidate without accepting caller-built outcomes.
    /// Retains immutable access to output and closure evidence. Does not mutate
    /// execution state, snapshot source, or grant native writer admission.
    /// # Errors
    /// Refuses failed output, missing closure, stale ownership or changed execution.
    pub async fn run_broker_candidate(
        &self,
        session: NativeBrokerSession,
        prepared: PreparedBrokerVendor,
        event: EffectAudit<'_>,
    ) -> Result<BrokerCandidate, BrokerCandidateFailure> {
        let packet = prepared.packet().clone();
        let (binding, _) = session.authority_binding();
        let outcome = self
            .run_broker_vendor(session, prepared, event)
            .await
            .map_err(BrokerCandidateFailure::before_outcome)?;
        let (output, receipt) = match (outcome.process, outcome.broker) {
            (Ok(output), Ok(receipt)) => (output, receipt),
            (process, broker) => {
                return Err(BrokerCandidateFailure::with_outcome(
                    refused(),
                    BrokerVendorOutcome { process, broker },
                ));
            }
        };
        match self
            .validate_broker_candidate(&packet, &binding, &output, &receipt)
            .await
        {
            Ok(candidate) => Ok(BrokerCandidate {
                binding,
                candidate,
                output,
                receipt,
            }),
            Err(cause) => Err(BrokerCandidateFailure::with_outcome(
                cause,
                BrokerVendorOutcome {
                    process: Ok(output),
                    broker: Ok(receipt),
                },
            )),
        }
    }

    async fn validate_broker_candidate(
        &self,
        packet: &ResumePacket,
        binding: &FileBrokerBinding,
        output: &ProcessOutput,
        receipt: &BrokerShutdownReceipt,
    ) -> Result<VendorCandidate, StoreError> {
        let candidate = interpret_vendor_output(packet, output).map_err(|_| refused())?;
        let _guard = self.mutations.lock().await;
        self.validate_runtime_owner(packet).await?;
        let (authority, generation) = self.store.runtime_authority().await?;
        let execution = self
            .store
            .execution(&binding.execution_token)
            .await?
            .ok_or_else(refused)?;
        let status = self.store.file_broker_status(binding).await?;
        let BrokerShutdownObservation::Revoked { completed_sequence } = receipt.observation else {
            return Err(refused());
        };
        if authority.as_deref() != Some(binding.authority_id.as_str())
            || generation != self.boot_generation
            || i64::try_from(generation).ok() != Some(binding.boot_generation)
            || execution.state != ExecutionState::Started
            || execution.wrapper_identity.as_deref() != Some(binding.invocation_id.as_str())
            || execution.preparation.packet_sha256 != candidate.packet_sha256
            || !status.closed
            || status.admitted != status.completed
            || u64::try_from(status.completed).ok() != Some(completed_sequence)
        {
            return Err(refused());
        }
        Ok(candidate)
    }
}
fn refused() -> StoreError {
    StoreError::Integrity("broker candidate lacks current bound completion evidence".into())
}
