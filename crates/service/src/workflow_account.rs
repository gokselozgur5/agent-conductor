//! Account worker selection for the normal persistent workflow dispatcher.
use crate::{Clock, NativeWorkflowPlan, PersistentControlService};
use agent_conductor_domain::{ActorRole, Blocker, CommandKind, TaskState};
use agent_conductor_store::StoreError;
use conductor_runtime::{AccountChannelAddress, ResumePacket, SnapshotManifest, VendorPin};
use serde::{Deserialize, Serialize};
use std::{
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountWorkerTarget {
    pub socket: PathBuf,
    /// Existing receiver-owned directory; permissions are managed by its operator.
    pub artifacts: PathBuf,
}
impl AccountWorkerTarget {
    pub(super) fn validate(&self) -> Result<(), StoreError> {
        if !clean_absolute(&self.socket)
            || self.socket.file_name().is_none()
            || self.socket.as_os_str().len() > 100
            || !clean_absolute(&self.artifacts)
        {
            return Err(invalid("invalid account worker target"));
        }
        Ok(())
    }
    pub(super) fn workspace(&self) -> PathBuf {
        self.artifacts
            .join(format!("baseline-{}", uuid::Uuid::now_v7()))
            .join("tree")
    }
}
fn clean_absolute(path: &Path) -> bool {
    path.is_absolute()
        && path.components().all(|part| {
            !matches!(
                part,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
}

impl<C: Clock> PersistentControlService<C> {
    // Capture failure is not proof of termination. Preserve execution evidence
    // and block only the still-active attempt that failed, under the lifecycle lock.
    pub(super) async fn block_failed_account_workflow(
        &self,
        packet: &ResumePacket,
    ) -> Result<(), StoreError> {
        let _guard = self.mutations.lock().await;
        let task = self.workflow_task(&packet.contract.task_id).await?;
        if !matches!(
            task.state,
            TaskState::Claimed | TaskState::Running | TaskState::Verifying
        ) || !task.attempt.as_ref().is_some_and(|attempt| {
            attempt.id.0 == packet.contract.attempt_id
                && attempt.lease.epoch == packet.contract.lease_epoch
        }) {
            return Ok(());
        }
        self.workflow_command_locked(
            &task.id.0,
            "conductor-account-workflow",
            ActorRole::System,
            CommandKind::Block { blocker: Blocker {
                kind: "account_workflow_failed".into(),
                detail: "account workspace staging, launch or capture failed".into(),
                resume_when: "inspect retained task and execution evidence before authorizing another attempt".into(),
            }},
            "account workflow failed without an accepted source",
        ).await?;
        Ok(())
    }

    pub(super) async fn stage_workflow_account(
        &self,
        plan: &NativeWorkflowPlan,
        packet: &ResumePacket,
        baseline: &SnapshotManifest,
        artifacts: &Path,
    ) -> Result<(), StoreError> {
        let target = plan
            .account_worker
            .as_ref()
            .ok_or_else(|| invalid("account worker target missing"))?;
        self.validate_runtime_owner(packet).await?;
        let destination = packet
            .contract
            .workspace
            .parent()
            .ok_or_else(|| invalid("account workspace missing"))?;
        let digest = baseline
            .sha256()
            .map_err(|_| invalid("baseline digest invalid"))?;
        let address = account_address(plan, digest)?;
        // Retain the chosen remote path before sending bytes so failed staging
        // remains inspectable and cannot silently select a different destination.
        let metadata = serde_json::to_vec(&serde_json::json!({"socket":target.socket,"workspace":packet.contract.workspace,"baseline":address.packet_sha256}))
            .map_err(|_| invalid("account workspace metadata invalid"))?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(artifacts.join("account-workspace.json"))
            .map_err(|_| invalid("account workspace metadata unavailable"))?;
        file.write_all(&metadata)
            .and_then(|()| file.sync_all())
            .map_err(|_| invalid("account workspace metadata failed"))?;
        conductor_runtime::stage_account_snapshot(
            &address,
            &artifacts.join("work/tree"),
            baseline,
            destination,
        )
        .await
        .map_err(|_| invalid("account baseline staging failed"))?;
        self.validate_runtime_owner(packet).await?;
        Ok(())
    }

    pub(super) async fn execute_account_workflow_writer(
        &self,
        plan: &NativeWorkflowPlan,
        pin: &VendorPin,
        packet: &ResumePacket,
        token: &str,
        baseline: &SnapshotManifest,
        artifacts: &Path,
    ) -> Result<SnapshotManifest, StoreError> {
        let address = account_address(
            plan,
            packet
                .sha256()
                .map_err(|_| invalid("account packet digest invalid"))?,
        )?;
        let intent = self
            .prepare_account_execution(packet, token, "native-workflow")
            .await?;
        {
            let _guard = self.mutations.lock().await;
            self.validate_runtime_owner(packet).await?;
            self.publish_owned_scan(token, packet, baseline.clone(), true)
                .await?;
        }
        let (_, connection) = self
            .release_account_execution(intent, &address, pin)
            .await?;
        let outcome = self.await_account_execution(token, connection).await?;
        let remote = packet
            .contract
            .workspace
            .parent()
            .ok_or_else(|| invalid("account baseline directory missing"))?
            .with_extension("candidate");
        let paths = baseline
            .entries
            .iter()
            .map(|entry| entry.path.clone())
            .collect::<Vec<_>>();
        self.finalize_account_source(
            outcome,
            &address,
            &remote,
            &artifacts.join("source"),
            &paths,
        )
        .await
    }
}
fn account_address(
    plan: &NativeWorkflowPlan,
    digest: String,
) -> Result<AccountChannelAddress, StoreError> {
    let target = plan
        .account_worker
        .as_ref()
        .ok_or_else(|| invalid("account worker target missing"))?;
    let profile = plan
        .full_capability
        .as_ref()
        .ok_or_else(|| invalid("account worker requires full capability profile"))?;
    Ok(AccountChannelAddress {
        path: target.socket.clone(),
        worker_uid: profile.account.uid,
        coordinator_uid: nix::unistd::geteuid().as_raw(),
        packet_sha256: digest,
    })
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}
