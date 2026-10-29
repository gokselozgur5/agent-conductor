//! Eligibility to inspect a copied candidate is distinct from permission to
//! reassign its original workspace. Unrestricted exits never become OS fences.
use crate::{Clock, PersistentControlService};
use agent_conductor_store::{ExecutionRecord, ExecutionState, StoreError};
use conductor_runtime::ResumePacket;

impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn validate_capture_eligibility(
        &self,
        record: &ExecutionRecord,
        packet: &ResumePacket,
    ) -> Result<(), StoreError> {
        let identity_matches = packet.sha256().ok().as_deref()
            == Some(&record.preparation.packet_sha256)
            && packet.contract.task_id == record.preparation.task_id
            && packet.contract.attempt_id == record.preparation.attempt_id
            && i64::try_from(packet.contract.lease_epoch).ok()
                == Some(record.preparation.lease_epoch);
        let exited_full = packet.contract.full_capability.is_some()
            && record.state == ExecutionState::Exited
            && record.exit_code == Some(0)
            && record.wrapper_identity.is_some();
        if !identity_matches
            || !(record.state == ExecutionState::Fenced || exited_full)
            || self
                .store
                .has_other_unfenced_execution(
                    &record.preparation.task_id,
                    &record.preparation.effect_token,
                )
                .await?
        {
            return Err(StoreError::Integrity(
                "candidate execution is not eligible for capture verification".into(),
            ));
        }
        Ok(())
    }
}
