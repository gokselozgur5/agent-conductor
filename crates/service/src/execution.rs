//! Internal controlled-runtime port. Not an operator command or vendor adapter.
use super::{Clock, PersistentControlService};
use agent_conductor_domain::{Task, TaskState};
use agent_conductor_store::{
    EffectAudit, ExecutionPreparation, ExecutionRecord, ExecutionState, ExecutionUpdate, StoreError,
};
use conductor_runtime::{
    AdapterKind, ResumePacket, RunnerConnection, RunnerConnectionError, RunnerEvent, RunnerLaunch,
};
use std::path::Path;
use uuid::Uuid;

impl<C: Clock> PersistentControlService<C> {
    /// Starts one controlled fake worker from an already claimed spawn effect.
    /// Preparation, current ownership checks and recorded acknowledgement precede
    /// release. The mutation lock orders release against task cancellation.
    /// Real vendor adapters are refused until their isolation/conformance gate.
    /// # Errors
    /// Refuses stale ownership, replay, unsupported adapters and protocol failures.
    /// Failed launch leaves durable intent for reconciliation, never deletes it.
    pub async fn launch_controlled_execution(
        &self,
        wrapper: &Path,
        launch: &RunnerLaunch,
        effect_token: &str,
        runner_id: &str,
    ) -> Result<(ExecutionRecord, RunnerConnection), StoreError> {
        if launch.packet.contract.adapter.kind != AdapterKind::Fake {
            return Err(runtime_error(
                "native adapters require a pinned launch entry",
            ));
        }
        self.launch_execution(wrapper, launch, effect_token, runner_id)
            .await
    }

    /// Launches a pinned native writer admitted by its runtime adapter. Its authentication
    /// environment is forwarded only through the private wrapper pipe, never SQL.
    /// Command-capable writers remain refused pending descendant fencing evidence.
    /// The pin must be selected by trusted daemon configuration, not worker text.
    /// # Errors
    /// Refuses unsupported writer capabilities, stale identity or failed release.
    pub async fn launch_native_execution(
        &self,
        wrapper: &Path,
        pin: &conductor_runtime::VendorPin,
        packet: &ResumePacket,
        authentication: std::collections::BTreeMap<String, String>,
        effect_token: &str,
        runner_id: &str,
    ) -> Result<(ExecutionRecord, RunnerConnection), StoreError> {
        let launch = conductor_runtime::native_writer_launch(pin, packet, authentication)
            .map_err(|_| runtime_error("native writer adapter refused the contract"))?;
        self.launch_execution(wrapper, &launch, effect_token, runner_id)
            .await
    }

    pub(super) async fn launch_execution(
        &self,
        wrapper: &Path,
        launch: &RunnerLaunch,
        effect_token: &str,
        runner_id: &str,
    ) -> Result<(ExecutionRecord, RunnerConnection), StoreError> {
        let _guard = self.mutations.lock().await;
        self.validate_runtime_owner(&launch.packet).await?;
        let packet_json =
            serde_json::to_string(&launch.packet).map_err(|_| runtime_error("packet encoding"))?;
        let input = ExecutionPreparation {
            effect_token: effect_token.into(),
            task_id: launch.packet.contract.task_id.clone(),
            attempt_id: launch.packet.contract.attempt_id.clone(),
            lease_epoch: i64::try_from(launch.packet.contract.lease_epoch)
                .map_err(|_| runtime_error("epoch overflow"))?,
            runner_id: runner_id.into(),
            packet_json,
            packet_sha256: launch
                .packet
                .sha256()
                .map_err(|_| runtime_error("packet hash"))?,
        };
        let audit_id = Uuid::now_v7().to_string();
        let now = self.clock.read(self.boot_generation);
        let (record, fresh) = self
            .store
            .prepare_execution_once(
                &input,
                EffectAudit {
                    id: &audit_id,
                    actor_id: "conductor-runtime",
                    correlation_id: effect_token,
                    now_ms: now.wall_unix_ms,
                },
            )
            .await?;
        if !fresh {
            return Err(runtime_error(
                "execution already exists; reconcile before retry",
            ));
        }
        let wrapper_identity = Uuid::now_v7().to_string();
        let mut acknowledged = None;
        let connection = RunnerConnection::launch(wrapper, launch, |ack| async {
            // The wrapper may take time to start. Recheck the monotonic lease,
            // not just the SQL active flag, immediately before recording its ACK.
            self.validate_runtime_owner(&launch.packet)
                .await
                .map_err(|_| RunnerConnectionError::Persistence)?;
            let audit_id = Uuid::now_v7().to_string();
            let now = self.clock.read(self.boot_generation);
            let result = self
                .store
                .update_execution(
                    effect_token,
                    record.revision,
                    &wrapper_identity,
                    &ExecutionUpdate::Acknowledge {
                        wrapper_identity: wrapper_identity.clone(),
                        packet_sha256: ack.packet_sha256,
                    },
                    EffectAudit {
                        id: &audit_id,
                        actor_id: "conductor-runtime",
                        correlation_id: effect_token,
                        now_ms: now.wall_unix_ms,
                    },
                )
                .await
                .map_err(|_| RunnerConnectionError::Persistence)?;
            // A slow database acknowledgement must not revive an expired lease.
            let deadline = self
                .validate_runtime_owner(&launch.packet)
                .await
                .map_err(|_| RunnerConnectionError::Persistence)?;
            acknowledged = Some(result);
            Ok(deadline)
        })
        .await
        .map_err(|_| runtime_error("wrapper release failed; reconcile durable execution"))?;
        Ok((
            acknowledged.ok_or_else(|| runtime_error("acknowledgement missing"))?,
            connection,
        ))
    }

    /// Renews only the current live execution under lifecycle serialization.
    /// # Errors
    /// Refuses ended/replaced/expired attempts or an unacknowledged execution.
    /// On refusal, the caller must drop/cancel the connection and reconcile.
    pub async fn renew_controlled_execution(
        &self,
        token: &str,
        wrapper_identity: &str,
        connection: &mut RunnerConnection,
    ) -> Result<(), StoreError> {
        let _guard = self.mutations.lock().await;
        let record = self
            .store
            .execution(token)
            .await?
            .ok_or_else(|| runtime_error("missing execution"))?;
        if record.preparation.packet_sha256 != connection.packet_sha256()
            || record.state != ExecutionState::Started
            || record.wrapper_identity.as_deref() != Some(wrapper_identity)
        {
            return Err(runtime_error("execution identity mismatch"));
        }
        let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
            .map_err(|_| runtime_error("invalid stored packet"))?;
        let deadline = self.validate_runtime_owner(&packet).await?;
        connection
            .renew(deadline)
            .await
            .map_err(|_| runtime_error("wrapper renewal failed"))
    }

    /// Drives a released native connection while renewing only current ownership.
    /// Records candidate exit or a blocked failure; it never completes the task.
    /// # Errors
    /// Refuses stale/cancelled ownership, malformed output and failed processes.
    /// Uncertain outcomes remain durable and block reassignment.
    pub async fn await_native_execution(
        &self,
        token: &str,
        wrapper_identity: &str,
        packet: &ResumePacket,
        mut connection: RunnerConnection,
    ) -> Result<(ExecutionRecord, conductor_runtime::VendorCandidate), StoreError> {
        if packet
            .sha256()
            .map_err(|_| runtime_error("invalid native packet"))?
            != connection.packet_sha256()
        {
            return Err(runtime_error(
                "native result packet differs from released packet",
            ));
        }
        let mut timer = tokio::time::interval(std::time::Duration::from_millis(200));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let terminal = loop {
            tokio::select! {
                event = connection.event() => break event.ok(),
                _ = timer.tick() => {
                    if self.renew_controlled_execution(token,wrapper_identity,&mut connection).await.is_err() {
                        let _ = self.stop_controlled_execution(token,wrapper_identity,connection).await;
                        return Err(runtime_error("native execution lost current task ownership"));
                    }
                }
            }
        };
        let settled = connection.wait().await.is_ok();
        let _guard = self.mutations.lock().await;
        let record = self
            .validate_execution_connection(token, wrapper_identity, &connection)
            .await?;
        let current = self.validate_runtime_owner(packet).await.is_ok();
        let candidate = match terminal {
            Some(RunnerEvent::Finished {
                exit_code,
                stdout,
                stderr,
            }) if settled && current => conductor_runtime::interpret_vendor_output(
                packet,
                &conductor_runtime::ProcessOutput {
                    exit_code,
                    stdout,
                    stderr,
                },
            )
            .ok(),
            _ => None,
        };
        let update = if candidate.is_some() {
            ExecutionUpdate::Exited {
                wrapper_identity: wrapper_identity.into(),
                exit_code: Some(0),
            }
        } else {
            ExecutionUpdate::Blocked {
                reason: "native terminal output or ownership was not accepted".into(),
            }
        };
        let audit_id = Uuid::now_v7().to_string();
        let record = self
            .store
            .update_execution(
                token,
                record.revision,
                &format!("native-result:{wrapper_identity}"),
                &update,
                EffectAudit {
                    id: &audit_id,
                    actor_id: "conductor-runtime",
                    correlation_id: token,
                    now_ms: self.clock.read(self.boot_generation).wall_unix_ms,
                },
            )
            .await?;
        Ok((
            record,
            candidate
                .ok_or_else(|| runtime_error("native result blocked; inspect execution record"))?,
        ))
    }

    /// Requests stop and records the observed result without completing the task.
    /// The five-second observation bound is not proof of death on timeout. Even a
    /// controlled group stop remains blocked until isolation/fence verification
    /// establishes that no writer escaped the group.
    /// # Errors
    /// Refuses a mismatched owner or database failure. Transport loss is persisted
    /// as blocked; the connection then closes so the independent watchdog runs.
    pub async fn stop_controlled_execution(
        &self,
        token: &str,
        wrapper_identity: &str,
        mut connection: RunnerConnection,
    ) -> Result<(ExecutionRecord, Option<RunnerEvent>), StoreError> {
        {
            let _guard = self.mutations.lock().await;
            self.validate_execution_connection(token, wrapper_identity, &connection)
                .await?;
        }
        let observation = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            connection.cancel().await?;
            let event = connection.event().await?;
            connection.wait().await?;
            Ok::<_, RunnerConnectionError>(event)
        })
        .await
        .ok()
        .and_then(Result::ok);
        let _guard = self.mutations.lock().await;
        let record = self
            .validate_execution_connection(token, wrapper_identity, &connection)
            .await?;
        let update = match observation.as_ref() {
            Some(RunnerEvent::Finished { exit_code, .. }) => ExecutionUpdate::Exited {
                wrapper_identity: wrapper_identity.into(), exit_code: *exit_code,
            },
            Some(RunnerEvent::Stopped { .. }) => ExecutionUpdate::Blocked {
                reason: "controlled group stopped; complete write fence is not yet verified".into(),
            },
            _ => ExecutionUpdate::Blocked {
                reason: "wrapper terminal observation unavailable; reconcile without PID-only signalling".into(),
            },
        };
        let audit_id = Uuid::now_v7().to_string();
        let record = self
            .store
            .update_execution(
                token,
                record.revision,
                &format!("stop:{wrapper_identity}"),
                &update,
                EffectAudit {
                    id: &audit_id,
                    actor_id: "conductor-runtime",
                    correlation_id: token,
                    now_ms: self.clock.read(self.boot_generation).wall_unix_ms,
                },
            )
            .await?;
        Ok((record, observation))
    }

    async fn validate_execution_connection(
        &self,
        token: &str,
        wrapper_identity: &str,
        connection: &RunnerConnection,
    ) -> Result<ExecutionRecord, StoreError> {
        let record = self
            .store
            .execution(token)
            .await?
            .ok_or_else(|| runtime_error("missing execution"))?;
        if record.state != ExecutionState::Started
            || record.wrapper_identity.as_deref() != Some(wrapper_identity)
            || record.preparation.packet_sha256 != connection.packet_sha256()
        {
            return Err(runtime_error("execution connection identity mismatch"));
        }
        Ok(record)
    }

    pub(super) async fn recover_runtime_observations(&self) -> Result<(), StoreError> {
        for record in self.store.unresolved_executions().await? {
            let token = &record.preparation.effect_token;
            let audit_id = Uuid::now_v7().to_string();
            self.store
                .update_execution(
                    token,
                    record.revision,
                    &format!("restart:{}", self.boot_generation),
                    &ExecutionUpdate::Blocked {
                        reason: "controller restarted; live wrapper ownership must be reconciled"
                            .into(),
                    },
                    EffectAudit {
                        id: &audit_id,
                        actor_id: "conductor-runtime",
                        correlation_id: token,
                        now_ms: self.clock.read(self.boot_generation).wall_unix_ms,
                    },
                )
                .await?;
        }
        // Also cover a crash after the record was blocked but before its task was
        // blocked. Scan task ownership, not just freshly changed execution rows.
        for row in self.store.tasks(None).await? {
            let task: Task = serde_json::from_str(&row.aggregate_json)
                .map_err(|_| runtime_error("invalid task aggregate"))?;
            if matches!(
                task.state,
                TaskState::Ready | TaskState::Claimed | TaskState::Running | TaskState::Verifying
            ) && self.store.has_unfenced_execution(&task.id.0).await?
            {
                self.block_effect_contradiction(
                    &task.id.0,
                    &format!("runtime-restart-fence:{}", task.version),
                    "controller restart left an execution without confirmed write fencing",
                )
                .await
                .map_err(|error| runtime_error(&error.message))?;
            }
        }
        Ok(())
    }

    pub(super) async fn validate_runtime_owner(
        &self,
        packet: &ResumePacket,
    ) -> Result<u64, StoreError> {
        packet
            .validate()
            .map_err(|_| runtime_error("invalid runtime contract"))?;
        let row = self
            .store
            .task(&packet.contract.task_id)
            .await?
            .ok_or_else(|| runtime_error("missing task"))?;
        let task: Task = serde_json::from_str(&row.aggregate_json)
            .map_err(|_| runtime_error("invalid task aggregate"))?;
        let attempt = task
            .attempt
            .as_ref()
            .ok_or_else(|| runtime_error("missing attempt"))?;
        // Sample the host clock first: converting remaining service-clock time
        // this way cannot extend the grant by time spent reading the service clock.
        let host_now = conductor_runtime::continuous_millis()
            .map_err(|_| runtime_error("host clock unavailable"))?;
        let now = self.clock.read(self.boot_generation);
        if !matches!(task.state, TaskState::Claimed | TaskState::Running)
            || attempt.id.0 != packet.contract.attempt_id
            || attempt.worker.0 != packet.contract.worker_id
            || attempt.lease.epoch != packet.contract.lease_epoch
            || !attempt.lease.accepts_worker_mutation(now)
            || task.required_checks.iter().any(|required| {
                !packet
                    .contract
                    .checks
                    .iter()
                    .any(|check| check.id == required.id.0)
            })
        {
            return Err(runtime_error("runtime attempt or lease is not current"));
        }
        let remaining = attempt
            .lease
            .worker_deadline_monotonic_ms
            .min(attempt.lease.deadline_monotonic_ms)
            .checked_sub(now.monotonic_ms)
            .ok_or_else(|| runtime_error("expired lease"))?;
        host_now
            .checked_add(remaining)
            .ok_or_else(|| runtime_error("deadline overflow"))
    }
}
fn runtime_error(reason: &str) -> StoreError {
    StoreError::Integrity(reason.into())
}
