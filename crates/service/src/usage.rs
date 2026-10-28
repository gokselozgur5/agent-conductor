use crate::{Clock, PersistentControlService};
use agent_conductor_store::StoreError;
use conductor_runtime::{ResumePacket, reported_usage};
use sha2::{Digest, Sha256};
impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn record_terminal_usage(
        &self,
        packet: &ResumePacket,
        invocation_id: &str,
        event: Option<&conductor_runtime::RunnerEvent>,
        settled: bool,
    ) -> Result<(), StoreError> {
        if settled
            && packet.contract.adapter.kind != conductor_runtime::AdapterKind::Fake
            && let Some(
                conductor_runtime::RunnerEvent::Finished { stdout, .. }
                | conductor_runtime::RunnerEvent::Stopped { stdout, .. },
            ) = event
        {
            self.record_vendor_usage(packet, "worker", invocation_id, stdout)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn record_vendor_usage(
        &self,
        packet: &ResumePacket,
        role: &str,
        invocation_id: &str,
        stdout: &[u8],
    ) -> Result<(), StoreError> {
        let packet_sha256 = packet
            .sha256()
            .map_err(|_| StoreError::Integrity("invalid usage packet".into()))?;
        let output_sha256 = format!("{:x}", Sha256::digest(stdout));
        let id = format!(
            "usage:{:x}",
            Sha256::digest(format!("{role}:{invocation_id}"))
        );
        let report = reported_usage(packet.contract.adapter.kind, stdout);
        let metadata = serde_json::json!({"version":1,"invocation_id":invocation_id,"task_id":packet.contract.task_id,"attempt_id":packet.contract.attempt_id,"lease_epoch":packet.contract.lease_epoch,"role":role,"agent_id":packet.contract.worker_id,"adapter":packet.contract.adapter,"packet_sha256":packet_sha256,"output_sha256":output_sha256,"report":report}).to_string();
        self.store
            .record_usage(
                &id,
                &packet.contract.task_id,
                &packet.contract.attempt_id,
                &metadata,
                self.clock.read(self.boot_generation).wall_unix_ms,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_conductor_store::{NewProject, NewTask, Store};
    #[tokio::test]
    async fn distinct_invocations_with_identical_output_are_not_deduplicated() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.sqlite3")).await.unwrap();
        store
            .create_project(NewProject {
                id: "project",
                canonical_path: "/workspace",
                filesystem_identity: "fixture",
                git_common_dir_identity: None,
                created_at_ms: 1,
            })
            .await
            .unwrap();
        let task = agent_conductor_domain::Task::new("task".into(), vec![]);
        store
            .create_task(NewTask {
                id: "task",
                project_id: "project",
                objective: "usage fixture",
                state: "backlog",
                priority: 0,
                authority_json: "{}",
                aggregate_json: &serde_json::to_string(&task).unwrap(),
                created_at_ms: 1,
            })
            .await
            .unwrap();
        let service = PersistentControlService::start(store.clone(), 1)
            .await
            .unwrap();
        let packet:ResumePacket=serde_json::from_value(serde_json::json!({
            "contract":{"version":1,"task_id":"task","attempt_id":"attempt","lease_epoch":1,"worker_id":"reviewer","adapter":{"kind":"codex","version":"fixture","executable_sha256":"a".repeat(64),"conformance_sha256":"b".repeat(64)},"workspace":"/workspace","starting_head":"c".repeat(40),"authority":{"read_roots":["/workspace"],"write_roots":[],"network":"inference_only","allow_workspace_commands":true},"instructions":[],"checks":[],"max_runtime_ms":10000},"objective":"fixture","checkpoint":null,"untrusted_context":[]
        })).unwrap();
        let output = b"{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":7}}\n";
        for invocation in ["call-a", "call-b", "call-a"] {
            service
                .record_vendor_usage(&packet, "reviewer", invocation, output)
                .await
                .unwrap();
        }
        let rows = store
            .inventory_page(agent_conductor_store::InventoryKind::Usage, None)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            service
                .record_vendor_usage(&packet, "reviewer", "call-a", b"different\n")
                .await
                .is_err()
        );
        for row in rows {
            let metadata: serde_json::Value = serde_json::from_str(&row.metadata).unwrap();
            assert_eq!(metadata["task_id"], "task");
            assert_eq!(metadata["attempt_id"], "attempt");
            assert_eq!(metadata["report"]["tokens"]["input_tokens"], "7");
        }
    }
}
