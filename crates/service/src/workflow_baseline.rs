//! Initial task evidence survives retained-work continuation without new authority.
use crate::{Clock, NativeWorkflowPlan, PersistentControlService};
use agent_conductor_store::{ExecutionRecord, ExecutionState, StoreError};
use conductor_runtime::{AdvisoryInventory, ResumePacket, SnapshotManifest};
use std::{
    fs::OpenOptions,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn review_baseline(
        &self,
        plan: &NativeWorkflowPlan,
        current: &SnapshotManifest,
        continuing: bool,
    ) -> Result<SnapshotManifest, StoreError> {
        if continuing {
            self.initial_workflow_baseline(plan).await
        } else {
            Ok(current.clone())
        }
    }

    pub(super) async fn initial_workflow_baseline(
        &self,
        plan: &NativeWorkflowPlan,
    ) -> Result<SnapshotManifest, StoreError> {
        let root = plan
            .artifacts
            .canonicalize()
            .map_err(|_| invalid("initial artifacts unavailable"))?;
        let workspace = root.join("work/tree");
        let record = self
            .store
            .execution_for_workspace(
                &plan.task_id,
                workspace
                    .to_str()
                    .ok_or_else(|| invalid("initial workspace encoding failed"))?,
            )
            .await?
            .ok_or_else(|| invalid("initial execution unavailable"))?;
        bound_initial_manifest(plan, &workspace, &record)
    }
}

fn bound_initial_manifest(
    plan: &NativeWorkflowPlan,
    workspace: &Path,
    record: &ExecutionRecord,
) -> Result<SnapshotManifest, StoreError> {
    let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
        .map_err(|_| invalid("initial packet invalid"))?;
    if record.state != ExecutionState::Fenced
        || record.preparation.task_id != plan.task_id
        || packet.contract.task_id != plan.task_id
        || packet.contract.workspace != workspace
        || packet.contract.attempt_id != record.preparation.attempt_id
        || i64::try_from(packet.contract.lease_epoch).ok() != Some(record.preparation.lease_epoch)
        || packet.contract.worker_id != format!("{}:worker", plan.task_id)
        || packet.contract.adapter.kind != plan.worker.kind
        || packet.contract.adapter.version != plan.worker.version
        || packet.contract.adapter.executable_sha256 != plan.worker.executable_sha256
        || packet.contract.adapter.conformance_sha256 != plan.worker.conformance_sha256
        || packet.objective != plan.objective
        || packet.contract.checks != plan.checks
        || packet.contract.instructions != plan.instructions
        || packet.contract.full_capability != plan.full_capability
        || packet
            .sha256()
            .map_err(|_| invalid("initial packet hash failed"))?
            != record.preparation.packet_sha256
    {
        return Err(invalid("initial execution binding mismatch"));
    }
    let inventories = packet
        .untrusted_context
        .iter()
        .filter_map(|text| serde_json::from_str::<AdvisoryInventory>(text).ok())
        .collect::<Vec<_>>();
    if inventories.len() != 1
        || inventories[0].version != 1
        || inventories[0].starting_head != packet.contract.starting_head
    {
        return Err(invalid("initial baseline identity unavailable"));
    }
    let path = workspace
        .parent()
        .ok_or_else(|| invalid("initial manifest path invalid"))?
        .join("manifest.json");
    read_initial_manifest(&path, &inventories[0].baseline_manifest_sha256)
}

fn read_initial_manifest(path: &Path, expected: &str) -> Result<SnapshotManifest, StoreError> {
    if path
        .canonicalize()
        .map_err(|_| invalid("initial manifest unavailable"))?
        != path
    {
        return Err(invalid("initial manifest is not canonical"));
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| invalid("initial manifest unavailable"))?;
    let metadata = file
        .metadata()
        .map_err(|_| invalid("initial manifest metadata unavailable"))?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.mode() & 0o077 != 0
        || metadata.len() > 4 * 1024 * 1024
    {
        return Err(invalid("initial manifest boundary refused"));
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(4 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("initial manifest read failed"))?;
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(invalid("initial manifest exceeds limit"));
    }
    let manifest: SnapshotManifest =
        serde_json::from_slice(&bytes).map_err(|_| invalid("initial manifest invalid"))?;
    if manifest
        .sha256()
        .map_err(|_| invalid("initial manifest hash failed"))?
        != expected
    {
        return Err(invalid("initial manifest changed"));
    }
    Ok(manifest)
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_conductor_store::ExecutionPreparation;
    use conductor_runtime::{GitSnapshot, snapshot_attempt_tree};
    use std::os::unix::fs::symlink;

    fn fixture(root: &Path) -> (NativeWorkflowPlan, ExecutionRecord, SnapshotManifest) {
        let original = root.join("original");
        std::fs::create_dir(&original).unwrap();
        std::fs::write(original.join("result.txt"), "initial\n").unwrap();
        let manifest = snapshot_attempt_tree(&original, &root.join("work"), &[]).unwrap();
        let pin = serde_json::json!({"kind":"claude","version":"fixture","executable":"/bin/true","executable_sha256":"a".repeat(64),"conformance_sha256":"b".repeat(64)});
        let plan: NativeWorkflowPlan = serde_json::from_value(serde_json::json!({
            "task_id":"task","project":original,"artifacts":root,"objective":"Preserve result.txt.",
            "wrapper":"/bin/true","worker":pin,"verifier":pin,"checks":[{"id":"check","argv":["/bin/true"],"timeout_ms":1000}],"model_timeout_ms":1000
        })).unwrap();
        let baseline = GitSnapshot {
            starting_head: "c".repeat(40),
            status_sha256: "d".repeat(64),
            manifest: manifest.clone(),
        };
        let packet: ResumePacket = serde_json::from_value(serde_json::json!({
            "contract":{"version":1,"task_id":"task","attempt_id":"attempt","lease_epoch":1,"worker_id":"task:worker",
                "adapter":{"kind":"claude","version":"fixture","executable_sha256":"a".repeat(64),"conformance_sha256":"b".repeat(64)},
                "workspace":root.join("work/tree"),"starting_head":baseline.starting_head,"authority":{"read_roots":[root.join("work/tree")],"write_roots":[root.join("work/tree")],"network":"inference_only","allow_workspace_commands":false},
                "instructions":[],"checks":plan.checks,"max_runtime_ms":1000},
            "objective":plan.objective,"checkpoint":null,
            "untrusted_context":[serde_json::to_string(&conductor_runtime::discover_advisory(&baseline).unwrap()).unwrap()]
        })).unwrap();
        let record = ExecutionRecord {
            account_channel: false,
            process: None,
            account_peer: None,
            preparation: ExecutionPreparation {
                expected_wrapper_identity: None,
                effect_token: "token".into(),
                task_id: "task".into(),
                attempt_id: "attempt".into(),
                lease_epoch: 1,
                runner_id: "runner".into(),
                packet_json: serde_json::to_string(&packet).unwrap(),
                packet_sha256: packet.sha256().unwrap(),
            },
            revision: 1,
            state: ExecutionState::Fenced,
            wrapper_identity: Some("wrapper".into()),
            checkpoint_json: None,
            exit_code: None,
        };
        (plan, record, manifest)
    }

    #[test]
    fn initial_binding_rejects_replacing_the_execution_profile() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let (mut plan, record, _) = fixture(&root);
        plan.full_capability = Some(conductor_runtime::FullCapabilityProfile {
            account: conductor_runtime::ExecutionAccount {
                name: "worker".into(),
                uid: 501,
                gid: 20,
                home: "/Users/worker".into(),
            },
            path: "/usr/bin:/bin".into(),
        });
        assert!(bound_initial_manifest(&plan, &root.join("work/tree"), &record).is_err());
    }

    #[test]
    fn initial_evidence_does_not_become_retained_work_or_lose_deleted_paths() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let (plan, record, initial) = fixture(&root);
        let workspace = root.join("work/tree");
        std::fs::remove_file(workspace.join("result.txt")).unwrap();
        std::fs::write(workspace.join("partial.txt"), "unfinished\n").unwrap();
        let retained = snapshot_attempt_tree(&workspace, &root.join("old-behavior"), &[]).unwrap();
        assert_ne!(
            retained, initial,
            "retained work cannot prove the initial preservation baseline"
        );
        let preserved = bound_initial_manifest(&plan, &workspace, &record).unwrap();
        assert_eq!(preserved, initial);
        let inventory = preserved
            .entries
            .iter()
            .map(|e| e.path.clone())
            .collect::<Vec<_>>();
        let next =
            snapshot_attempt_tree(&workspace, &root.join("continuation"), &inventory).unwrap();
        assert!(
            next.entries
                .iter()
                .any(|e| e.path == "result.txt" && e.sha256.is_none())
        );
        assert_eq!(
            std::fs::read(root.join("continuation/tree/partial.txt")).unwrap(),
            b"unfinished\n"
        );
        assert_eq!(
            std::fs::read(workspace.join("partial.txt")).unwrap(),
            b"unfinished\n"
        );
        assert_eq!(
            bound_initial_manifest(&plan, &workspace, &record).unwrap(),
            initial
        );
    }

    #[test]
    fn initial_evidence_rejects_changed_missing_linked_and_wrongly_bound_inputs() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let (plan, record, initial) = fixture(&root);
        let workspace = root.join("work/tree");
        for change in 0..4 {
            let mut bad = record.clone();
            match change {
                0 => bad.state = ExecutionState::Started,
                1 => bad.preparation.task_id = "other".into(),
                2 => bad.preparation.packet_sha256 = "0".repeat(64),
                _ => bad.preparation.lease_epoch = 2,
            }
            assert!(bound_initial_manifest(&plan, &workspace, &bad).is_err());
        }
        let mut legacy = record.clone();
        let mut packet = ResumePacket::decode(legacy.preparation.packet_json.as_bytes()).unwrap();
        packet.untrusted_context.clear();
        legacy.preparation.packet_json = serde_json::to_string(&packet).unwrap();
        legacy.preparation.packet_sha256 = packet.sha256().unwrap();
        assert!(
            bound_initial_manifest(&plan, &workspace, &legacy)
                .unwrap_err()
                .to_string()
                .contains("initial baseline identity unavailable")
        );
        let path = root.join("work/manifest.json");
        let bytes = std::fs::read(&path).unwrap();
        let mut substituted = initial;
        substituted.entries[0].sha256 = Some("0".repeat(64));
        std::fs::write(&path, serde_json::to_vec(&substituted).unwrap()).unwrap();
        assert!(
            bound_initial_manifest(&plan, &workspace, &record)
                .unwrap_err()
                .to_string()
                .contains("initial manifest changed")
        );
        std::fs::write(&path, "{}").unwrap();
        assert!(bound_initial_manifest(&plan, &workspace, &record).is_err());
        std::fs::write(&path, &bytes).unwrap();
        std::fs::rename(&path, root.join("saved-manifest")).unwrap();
        assert!(bound_initial_manifest(&plan, &workspace, &record).is_err());
        symlink(root.join("saved-manifest"), &path).unwrap();
        assert!(bound_initial_manifest(&plan, &workspace, &record).is_err());
        assert_eq!(std::fs::read(root.join("saved-manifest")).unwrap(), bytes);
    }
}
