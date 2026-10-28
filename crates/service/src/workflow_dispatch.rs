//! Plan admission and one dispatch step, composed with the native workflow.
use crate::{Clock, NativeWorkflowPlan, PersistentControlService, json_error, store_error};
use agent_conductor_store::StoreError;
use conductor_protocol::{ErrorBody, ErrorCode, Response};
use std::collections::BTreeMap;

impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn submit_workflow(&self, input: &str) -> Result<Response, ErrorBody> {
        if input.len() > 131_072 {
            return Err(ErrorBody::new(
                ErrorCode::Validation,
                "plan exceeds 128 KiB",
            ));
        }
        let plan: NativeWorkflowPlan = serde_json::from_str(input)
            .map_err(|_| ErrorBody::new(ErrorCode::Validation, "invalid native plan"))?;
        crate::workflow::validate_plan(&plan).map_err(store_error)?;
        let canonical = serde_json::to_string(&plan).map_err(json_error)?;
        self.store
            .enqueue_workflow(
                &plan.task_id,
                &canonical,
                self.clock.read(self.boot_generation).wall_unix_ms,
            )
            .await
            .map_err(store_error)?;
        Ok(Response::WorkflowQueued {
            task_id: plan.task_id,
        })
    }
    /// Executes at most one durable dispatch. The host supplies ephemeral
    /// authentication; plan persistence cannot add environment credentials.
    /// # Errors
    /// Returns storage/ownership errors; unsuccessful work retains artifacts.
    pub async fn dispatch_next_workflow(
        &self,
        worker_auth: BTreeMap<String, String>,
        codex_auth: BTreeMap<String, String>,
    ) -> Result<bool, StoreError> {
        let Some(job) = self
            .store
            .claim_workflow(
                self.boot_generation,
                self.clock.read(self.boot_generation).wall_unix_ms,
            )
            .await?
        else {
            return Ok(false);
        };
        let result = match serde_json::from_str::<NativeWorkflowPlan>(&job.plan_json) {
            Ok(plan) => {
                let verifier_auth = if plan.verifier.kind == conductor_runtime::AdapterKind::Claude
                {
                    worker_auth.clone()
                } else {
                    codex_auth
                };
                let execution = if let Some(version) = job.resume_version {
                    self.continue_native_workflow(
                        &plan,
                        u64::try_from(version).map_err(|_| {
                            StoreError::Integrity("invalid continuation version".into())
                        })?,
                        worker_auth,
                        verifier_auth,
                    )
                    .await
                } else {
                    self.run_native_workflow(&plan, worker_auth, verifier_auth)
                        .await
                };
                match execution {
                    Ok(outcome) => {
                        serde_json::json!({"completed":outcome.completed,"source":outcome.source})
                    }
                    Err(_) => {
                        serde_json::json!({"completed":false,"reason":"workflow stopped; inspect retained task and artifacts"})
                    }
                }
            }
            Err(_) => serde_json::json!({"completed":false,"reason":"stored plan invalid"}),
        };
        self.store
            .finish_workflow(
                &job.task_id,
                self.boot_generation,
                &result.to_string(),
                self.clock.read(self.boot_generation).wall_unix_ms,
            )
            .await?;
        Ok(true)
    }
}
