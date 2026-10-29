//! Explicit dependency admission and source-bound input copying.
use crate::{Clock, NativeWorkflowPlan, PersistentControlService};
use agent_conductor_domain::{Task, TaskState};
use agent_conductor_store::StoreError;
use conductor_runtime::{
    AdapterKind, GitSnapshot, ResumePacket, SnapshotManifest, snapshot_attempt_tree,
};
use std::{
    fs::OpenOptions,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn validate_workflow_dependencies(
        &self,
        plan: &NativeWorkflowPlan,
    ) -> Result<(), StoreError> {
        let dependencies = self.store.workflow_dependencies(&plan.task_id).await?;
        if dependencies.len() != plan.depends_on.len() {
            return Err(invalid("dependency plan has no matching durable admission"));
        }
        if !dependencies.is_empty() {
            let stored = self
                .store
                .workflow_plan(&plan.task_id)
                .await?
                .ok_or_else(|| invalid("dependency plan missing"))?;
            let stored: NativeWorkflowPlan =
                serde_json::from_str(&stored).map_err(|_| invalid("dependency plan invalid"))?;
            if serde_json::to_value(stored).map_err(|_| invalid("stored plan encoding failed"))?
                != serde_json::to_value(plan).map_err(|_| invalid("plan encoding failed"))?
            {
                return Err(invalid("dependency plan differs from admitted plan"));
            }
        }
        let project_id = if dependencies.is_empty() {
            None
        } else {
            let registration = crate::trusted_project_registration(
                "candidate".into(),
                &plan.project.to_string_lossy(),
                self.clock.read(self.boot_generation).wall_unix_ms,
            )
            .map_err(|_| invalid("dependency project unavailable"))?;
            self.store
                .project_for_identity(
                    &registration.filesystem_identity,
                    registration.git_common_dir_identity.as_deref(),
                )
                .await?
        };
        for dependency in dependencies {
            if !plan.depends_on.contains(&dependency.predecessor_task_id) {
                return Err(invalid("dependency identity mismatch"));
            }
            let source = dependency
                .accepted_source
                .ok_or_else(|| invalid("dependency not admitted"))?;
            let row = self
                .store
                .task(&dependency.predecessor_task_id)
                .await?
                .ok_or_else(|| invalid("dependency task missing"))?;
            if project_id.as_deref() != Some(row.project_id.as_str()) {
                return Err(invalid("dependency project identity mismatch"));
            }
            let task = self.workflow_task(&dependency.predecessor_task_id).await?;
            if task.id.0 != dependency.predecessor_task_id {
                return Err(invalid("dependency aggregate identity mismatch"));
            }
            accepted_source(&task, &source)?;
        }
        Ok(())
    }

    pub(super) async fn snapshot_accepted_input(
        &self,
        plan: &NativeWorkflowPlan,
        predecessor: &str,
        destination: &Path,
    ) -> Result<GitSnapshot, StoreError> {
        self.validate_workflow_dependencies(plan).await?;
        let dependency = self
            .store
            .workflow_dependencies(&plan.task_id)
            .await?
            .into_iter()
            .find(|d| d.predecessor_task_id == predecessor)
            .ok_or_else(|| invalid("input dependency missing"))?;
        let source = dependency
            .accepted_source
            .ok_or_else(|| invalid("input not admitted"))?;
        let task = self.workflow_task(predecessor).await?;
        accepted_source(&task, &source)?;
        let upstream: NativeWorkflowPlan = serde_json::from_str(
            &self
                .store
                .workflow_plan(predecessor)
                .await?
                .ok_or_else(|| invalid("input plan missing"))?,
        )
        .map_err(|_| invalid("input plan invalid"))?;
        if upstream.task_id != predecessor || upstream.project != plan.project {
            return Err(invalid("input project mismatch"));
        }
        let attempt = task
            .attempt
            .as_ref()
            .ok_or_else(|| invalid("input attempt missing"))?;
        let record = self
            .store
            .execution_for_attempt(&attempt.id.0)
            .await?
            .ok_or_else(|| invalid("input execution missing"))?;
        let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
            .map_err(|_| invalid("input packet invalid"))?;
        self.validate_capture_eligibility(&record, &packet).await?;
        if record.preparation.task_id != predecessor
            || record.preparation.attempt_id != attempt.id.0
            || record.preparation.lease_epoch
                != i64::try_from(attempt.lease.epoch)
                    .map_err(|_| invalid("input epoch overflow"))?
            || packet.contract.adapter.kind == AdapterKind::Fake
            || packet.contract.task_id != predecessor
            || packet.contract.attempt_id != attempt.id.0
            || packet.contract.lease_epoch != attempt.lease.epoch
            || packet.contract.worker_id != attempt.worker.0
            || packet.contract.checks != upstream.checks
            || packet.contract.full_capability != upstream.full_capability
            || packet
                .sha256()
                .map_err(|_| invalid("input packet hash failed"))?
                != record.preparation.packet_sha256
        {
            return Err(invalid("input execution binding mismatch"));
        }
        let workspace = &packet.contract.workspace;
        let root = workspace
            .parent()
            .and_then(Path::parent)
            .ok_or_else(|| invalid("input artifact path invalid"))?;
        let approved_root = upstream
            .artifacts
            .canonicalize()
            .map_err(|_| invalid("input artifacts unavailable"))?;
        if root.join("work/tree") != *workspace
            || !root.starts_with(&approved_root)
            || root
                .canonicalize()
                .map_err(|_| invalid("input artifact root unavailable"))?
                != root
        {
            return Err(invalid("input artifact root mismatch"));
        }
        let capture = root.join("source");
        let destination = destination.to_owned();
        let starting_head = packet.contract.starting_head;
        tokio::task::spawn_blocking(move || {
            copy_accepted_source(&capture, &destination, &source, starting_head)
        })
        .await
        .map_err(|_| invalid("input copy task failed"))?
    }
}

fn accepted_source(task: &Task, source: &str) -> Result<(), StoreError> {
    let valid = task.state == TaskState::Completed
        && task.verification.as_ref().is_some_and(|v| {
            v.source.as_ref().is_some_and(|s| s.0 == source)
                && v.verdict.as_ref().is_some_and(|verdict| {
                    verdict.accepted
                        && verdict.source.0 == source
                        && task
                            .attempt
                            .as_ref()
                            .is_some_and(|a| a.worker != verdict.verifier)
                })
        });
    if valid {
        Ok(())
    } else {
        Err(invalid(
            "dependency requires completed independent source acceptance",
        ))
    }
}

fn copy_accepted_source(
    capture: &Path,
    destination: &Path,
    expected: &str,
    starting_head: String,
) -> Result<GitSnapshot, StoreError> {
    let tree = capture.join("tree");
    if tree
        .canonicalize()
        .map_err(|_| invalid("accepted source missing"))?
        != tree
    {
        return Err(invalid("accepted source is not canonical"));
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(capture.join("manifest.json"))
        .map_err(|_| invalid("accepted manifest unavailable"))?;
    let metadata = file
        .metadata()
        .map_err(|_| invalid("accepted manifest metadata unavailable"))?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.len() > 4 * 1024 * 1024
        || metadata.mode() & 0o077 != 0
    {
        return Err(invalid("accepted manifest boundary refused"));
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(4 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("accepted manifest read failed"))?;
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(invalid("accepted manifest too large"));
    }
    let manifest: SnapshotManifest =
        serde_json::from_slice(&bytes).map_err(|_| invalid("accepted manifest invalid"))?;
    if format!(
        "snapshot:{}",
        manifest
            .sha256()
            .map_err(|_| invalid("accepted manifest hash failed"))?
    ) != expected
    {
        return Err(invalid("accepted manifest changed"));
    }
    let inventory = manifest
        .entries
        .iter()
        .map(|e| e.path.clone())
        .collect::<Vec<_>>();
    let copied = snapshot_attempt_tree(&tree, destination, &inventory)
        .map_err(|_| invalid("accepted source copy failed"))?;
    if copied != manifest {
        return Err(invalid("accepted source bytes changed"));
    }
    Ok(GitSnapshot {
        starting_head,
        status_sha256: copied.sha256().map_err(|_| invalid("input hash failed"))?,
        manifest: copied,
    })
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    #[test]
    fn accepted_copy_preserves_deletions_and_refuses_tampering_and_links() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let source = root.join("original");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("answer.txt"), b"42\n").unwrap();
        let capture = root.join("source");
        let manifest = snapshot_attempt_tree(&source, &capture, &["deleted.txt".into()]).unwrap();
        let expected = format!("snapshot:{}", manifest.sha256().unwrap());
        let input =
            copy_accepted_source(&capture, &root.join("work"), &expected, "head".into()).unwrap();
        assert_eq!(input.manifest, manifest);
        assert_eq!(
            std::fs::read(root.join("work/tree/answer.txt")).unwrap(),
            b"42\n"
        );
        assert!(!root.join("work/tree/deleted.txt").exists());
        std::fs::write(capture.join("tree/answer.txt"), b"changed").unwrap();
        assert!(
            copy_accepted_source(&capture, &root.join("changed"), &expected, "head".into())
                .is_err()
        );
        std::fs::write(capture.join("tree/answer.txt"), b"42\n").unwrap();
        symlink(&capture, root.join("alias")).unwrap();
        assert!(
            copy_accepted_source(
                &root.join("alias"),
                &root.join("linked-root"),
                &expected,
                "head".into()
            )
            .is_err()
        );
        std::fs::rename(
            capture.join("manifest.json"),
            capture.join("original-manifest"),
        )
        .unwrap();
        symlink(
            capture.join("original-manifest"),
            capture.join("manifest.json"),
        )
        .unwrap();
        assert!(
            copy_accepted_source(
                &capture,
                &root.join("linked-manifest"),
                &expected,
                "head".into()
            )
            .is_err()
        );
    }
}
