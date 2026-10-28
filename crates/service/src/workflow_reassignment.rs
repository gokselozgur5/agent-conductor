//! Operator-selected successors retain the immutable task plan and prior evidence.
use crate::{Clock, NativeWorkflowPlan, PersistentControlService};
use agent_conductor_store::StoreError;
use conductor_runtime::{AdapterKind, VendorPin};
use std::collections::BTreeMap;

impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn selected_worker(
        &self,
        plan: &NativeWorkflowPlan,
    ) -> Result<(String, VendorPin), StoreError> {
        let Some((id, json)) = self.store.workflow_assignment(&plan.task_id).await? else {
            return Ok((format!("{}:worker", plan.task_id), plan.worker.clone()));
        };
        let pin: VendorPin =
            serde_json::from_str(&json).map_err(|_| invalid("invalid successor pin"))?;
        self.validate_successor(plan, &id, &pin).await?;
        Ok((id, pin))
    }

    async fn validate_successor(
        &self,
        plan: &NativeWorkflowPlan,
        worker: &str,
        pin: &VendorPin,
    ) -> Result<(), StoreError> {
        if worker.trim().is_empty()
            || worker.len() > 128
            || worker == format!("{}:verifier", plan.task_id)
            || pin.kind != AdapterKind::Claude
        {
            return Err(invalid("unsupported successor identity or capability"));
        }
        let agent = self
            .store
            .agent(worker)
            .await?
            .ok_or_else(|| invalid("successor must already be registered"))?;
        if agent.access_mode != "read_write"
            || agent.adapter_kind != "claude"
            || agent.protocol_fingerprint != pin.conformance_sha256
            || agent.capability_hash != pin.executable_sha256
        {
            return Err(invalid("successor differs from registered capabilities"));
        }
        Ok(())
    }

    pub(super) async fn queue_reassignment(
        &self,
        task_id: &str,
        version: u64,
        worker: &str,
        pin_json: &str,
    ) -> Result<(), StoreError> {
        if pin_json.len() > 4096 {
            return Err(invalid("successor pin exceeds bounds"));
        }
        let pin: VendorPin =
            serde_json::from_str(pin_json).map_err(|_| invalid("invalid successor pin"))?;
        let _guard = self.mutations.lock().await;
        let (task, mut packet) = self.continuation_packet(task_id, version).await?;
        if packet.contract.worker_id == worker {
            return Err(invalid("successor must differ from previous worker"));
        }
        let json = self
            .store
            .workflow_plan(task_id)
            .await?
            .ok_or_else(|| invalid("workflow plan missing"))?;
        let plan: NativeWorkflowPlan =
            serde_json::from_str(&json).map_err(|_| invalid("workflow plan invalid"))?;
        self.validate_successor(&plan, worker, &pin).await?;
        self.initial_workflow_baseline(&plan).await?;
        packet.contract.worker_id = worker.into();
        packet.contract.adapter = conductor_runtime::AdapterIdentity {
            kind: pin.kind,
            version: pin.version.clone(),
            executable_sha256: pin.executable_sha256.clone(),
            conformance_sha256: pin.conformance_sha256.clone(),
        };
        // Validate the actual executable and existing file-only capabilities before
        // durable queue admission. Authentication is supplied only by the daemon.
        conductor_runtime::native_writer_launch(&pin, &packet, BTreeMap::new())
            .map_err(|_| invalid("successor adapter refused retained contract"))?;
        let source = task
            .attempt
            .ok_or_else(|| invalid("source attempt missing"))?
            .id
            .0;
        let canonical =
            serde_json::to_string(&pin).map_err(|_| invalid("successor pin encoding"))?;
        self.store
            .reassign_workflow(
                task_id,
                version,
                worker,
                &canonical,
                &source,
                self.clock.read(self.boot_generation).wall_unix_ms,
            )
            .await
    }
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}
