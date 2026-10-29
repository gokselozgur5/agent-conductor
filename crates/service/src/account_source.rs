//! Bind an imported account candidate to independent verification.
use crate::{AccountExecutionOutcome, Clock, PersistentControlService, account_result::invalid};
use agent_conductor_store::{ExecutionUpdate, StoreError};
use conductor_runtime::{AccountChannelAddress, SnapshotManifest};
use std::path::Path;
use uuid::Uuid;

impl<C: Clock> PersistentControlService<C> {
    /// Capture the reported account output and import its selected exact bytes.
    /// Only that imported digest becomes eligible for independent checks/review.
    /// The original account execution remains Started and unfenced.
    /// # Errors
    /// Refuses changed evidence, stale leases/peers, transfer failure or publication conflict.
    pub async fn finalize_account_source(
        &self,
        outcome: AccountExecutionOutcome,
        address: &AccountChannelAddress,
        remote_destination: &Path,
        local_destination: &Path,
        baseline_paths: &[String],
    ) -> Result<SnapshotManifest, StoreError> {
        let _guard = self.mutations.lock().await;
        let token = &outcome.record.preparation.effect_token;
        if self.store.execution(token).await?.as_ref() != Some(&outcome.record) {
            return Err(invalid("account result changed"));
        }
        let peer = outcome
            .record
            .account_peer
            .as_ref()
            .ok_or_else(|| invalid("account peer missing"))?;
        let (authority, generation) = self.store.runtime_authority().await?;
        if authority.as_deref() != Some(&peer.authority_id)
            || generation != self.boot_generation
            || peer.boot_generation != self.boot_generation
        {
            return Err(invalid("account capture belongs to stale authority"));
        }
        if address.worker_uid != peer.worker_uid
            || address.coordinator_uid != peer.coordinator_uid
            || address.packet_sha256 != outcome.record.preparation.packet_sha256
        {
            return Err(invalid("account capture peer mismatch"));
        }
        self.validate_runtime_owner(&outcome.packet).await?;
        let request = conductor_runtime::AccountCaptureRequest {
            packet_sha256: address.packet_sha256.clone(),
            source: outcome.packet.contract.workspace.clone(),
            destination: remote_destination.into(),
            baseline_paths: baseline_paths.to_vec(),
        };
        let receipt = conductor_runtime::capture_account_candidate(address, &request)
            .await
            .map_err(|_| invalid("account capture failed"))?;
        self.validate_runtime_owner(&outcome.packet).await?;
        let mut selected = address.clone();
        selected.packet_sha256 = receipt.snapshot_sha256;
        let manifest = conductor_runtime::fetch_account_snapshot(
            &selected,
            remote_destination,
            local_destination,
        )
        .await
        .map_err(|_| invalid("account candidate import failed"))?;
        self.validate_runtime_owner(&outcome.packet).await?;
        let record = self
            .account_update(
                &outcome.record,
                ExecutionUpdate::CaptureAccount {
                    wrapper_identity: outcome
                        .record
                        .wrapper_identity
                        .clone()
                        .ok_or_else(|| invalid("account wrapper missing"))?,
                    output_sha256: outcome.candidate.output_sha256,
                    snapshot_sha256: selected.packet_sha256.clone(),
                },
            )
            .await?;
        self.validate_capture_eligibility(&record, &outcome.packet)
            .await?;
        self.publish_final_owned_capture(token, &outcome.packet, &manifest)
            .await?;
        self.publish_captured_source(
            &outcome.packet,
            token,
            &Uuid::now_v7().to_string(),
            agent_conductor_domain::SourceIdentity(format!("snapshot:{}", selected.packet_sha256)),
        )
        .await?;
        Ok(manifest)
    }
}
