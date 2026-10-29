mod broker_ownership;
pub use broker_ownership::{close_broker_from_receipt, record_broker_owner};
#[cfg(unix)]
mod broker_journal;
#[cfg(unix)]
pub use broker_journal::RegisteredBrokerJournal;
use std::{
    fs,
    future::Future,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Instant,
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use agent_conductor_domain::{
    Actor, ActorId, ActorRole, ClockReading, Command, CommandKind, CorrelationId, EffectIntentSpec,
    EffectKind, IdempotencyKey, LeaseStatus, Rejection, Task, TaskId, TaskState, apply,
};
use agent_conductor_store::{
    AgentRegistration, AttemptLeaseMutation, AuditAppend, CommitBatch, CommitResult,
    ConsumerStateEffect, EffectIntentAppend, EventDelivery, EventLimits, EventSourceIdentity,
    EventStoreError, EventSubscription, PersistedTask, ProjectRegistration, PublishResult,
    RuntimeInvariantViolation, SemanticEvent, Store, StoreError, TaskMutation, TaskRegistration,
    TaskTransitionAppend,
};
use conductor_protocol::{
    CommandAppliedResponse, ErrorBody, ErrorCode, EventDeliveryResponse, EventPublishedResponse,
    HealthResponse, HealthStatus, InvariantResponse, InvariantViolation, Request, Response,
    SemanticEventResponse, TaskResponse, WatcherStatus,
};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

#[cfg(unix)]
mod auxiliary;
mod auxiliary_supervision;
mod check_execution;
#[cfg(unix)]
mod checkpoint;
mod clock;
mod evidence;
#[cfg(unix)]
mod instruction_policy;
mod inventory;
mod lease_observation;
#[cfg(unix)]
mod repository_watch;
#[cfg(unix)]
mod usage;
#[cfg(unix)]
mod workflow;
#[cfg(unix)]
mod workflow_baseline;
#[cfg(unix)]
mod workflow_continuation;
#[cfg(unix)]
mod workflow_dependencies;
#[cfg(unix)]
mod workflow_dispatch;
#[cfg(unix)]
mod workflow_reassignment;
#[cfg(unix)]
mod workflow_review;
#[cfg(unix)]
pub use repository_watch::{OwnedWatchState, RepositoryWatch, RepositoryWatchState};
#[cfg(unix)]
pub use workflow::{InitialDecision, NativeWorkflowPlan, WorkflowOutcome};
#[cfg(unix)]
mod source_finalization;
#[cfg(unix)]
pub use check_execution::CheckWorkspace;
#[cfg(unix)]
mod execution;
pub use clock::SystemClock;

pub trait ControlService: Send + Sync + 'static {
    fn handle(&self, request: Request) -> impl Future<Output = Result<Response, ErrorBody>> + Send;
    fn maintain(&self) -> impl Future<Output = Result<(), ErrorBody>> + Send {
        async { Ok(()) }
    }
}
pub trait Clock: Send + Sync + 'static {
    fn read(&self, boot_generation: u64) -> ClockReading;
}

pub struct PersistentControlService<C = SystemClock> {
    store: Store,
    boot_generation: u64,
    started_at: Instant,
    clock: Arc<C>,
    mutations: Arc<Mutex<()>>,
    auxiliary_recovery_cursor: Arc<Mutex<Option<String>>>,
    watcher_status: Arc<RwLock<WatcherStatus>>,
    observed_projects: Arc<RwLock<std::collections::BTreeMap<String, String>>>,
}

impl<C: Clock> Clone for PersistentControlService<C> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            boot_generation: self.boot_generation,
            started_at: self.started_at,
            clock: Arc::clone(&self.clock),
            mutations: Arc::clone(&self.mutations),
            auxiliary_recovery_cursor: Arc::clone(&self.auxiliary_recovery_cursor),
            watcher_status: Arc::clone(&self.watcher_status),
            observed_projects: Arc::clone(&self.observed_projects),
        }
    }
}

impl PersistentControlService<SystemClock> {
    /// Starts after durable integrity and restart recovery checks.
    /// # Errors
    /// Returns an error when durable startup cannot complete.
    pub async fn start(store: Store, started_at_ms: i64) -> Result<Self, StoreError> {
        let clock = SystemClock::try_new()
            .map_err(|error| StoreError::Integrity(format!("host clock unavailable: {error}")))?;
        Self::start_with_clock(store, started_at_ms, clock).await
    }

    /// Starts using a durably reserved external authority generation.
    /// # Errors
    /// Refuses invalid reservations or failed durable recovery checks.
    pub async fn start_reserved(
        store: Store,
        started_at_ms: i64,
        authority_id: &str,
        generation: u64,
    ) -> Result<Self, StoreError> {
        let clock = SystemClock::try_new()
            .map_err(|error| StoreError::Integrity(format!("host clock unavailable: {error}")))?;
        Self::start_with_reservation(
            store,
            started_at_ms,
            clock,
            Some((authority_id, generation)),
        )
        .await
    }
}

impl<C: Clock> PersistentControlService<C> {
    /// Updates process-local watcher health without changing durable task truth.
    pub fn set_watcher_status(&self, status: WatcherStatus) {
        if let Ok(mut current) = self.watcher_status.write() {
            *current = status;
        }
    }

    /// Starts with an injected authoritative clock.
    /// # Errors
    /// Returns an error when durable startup or recovery cannot complete.
    pub async fn start_with_clock(
        store: Store,
        started_at_ms: i64,
        clock: C,
    ) -> Result<Self, StoreError> {
        Self::start_with_reservation(store, started_at_ms, clock, None).await
    }

    async fn start_with_reservation(
        store: Store,
        started_at_ms: i64,
        clock: C,
        reservation: Option<(&str, u64)>,
    ) -> Result<Self, StoreError> {
        let startup = match reservation {
            Some((identity, generation)) => {
                store
                    .start_reserved_runtime(started_at_ms, identity, generation)
                    .await?
            }
            None => store.start_runtime(started_at_ms).await?,
        };
        store
            .recover_workflow_dispatch(startup.boot_generation, started_at_ms)
            .await?;
        store
            .reconcile_completed_dispatch(startup.boot_generation, started_at_ms)
            .await?;
        let service = Self {
            store,
            boot_generation: startup.boot_generation,
            started_at: Instant::now(),
            clock: Arc::new(clock),
            mutations: Arc::new(Mutex::new(())),
            auxiliary_recovery_cursor: Arc::new(Mutex::new(None)),
            watcher_status: Arc::new(RwLock::new(WatcherStatus::Disabled)),
            observed_projects: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
        };
        for lease in startup.recovery_pending {
            let Some(row) = service.store.task(&lease.task_id).await? else {
                continue;
            };
            let task: Task = serde_json::from_str(&row.aggregate_json)
                .map_err(|e| StoreError::Integrity(format!("invalid aggregate: {e}")))?;
            if task.attempt.as_ref().is_some_and(|a| {
                matches!(
                    a.lease.status,
                    LeaseStatus::Active | LeaseStatus::RecoveryPending
                )
            }) {
                let command = Command {
                    actor: Actor {
                        id: ActorId("conductor-runtime".into()),
                        role: ActorRole::System,
                    },
                    expected_version: task.version,
                    correlation_id: CorrelationId(format!("boot-{}", startup.boot_generation)),
                    idempotency_key: IdempotencyKey(format!(
                        "recovery:{}:{}",
                        startup.boot_generation, task.id.0
                    )),
                    reason: "daemon restart fenced active lease".into(),
                    kind: CommandKind::EnterRecovery,
                };
                service
                    .apply_row(row, command)
                    .await
                    .map_err(|e| StoreError::Integrity(e.message))?;
            }
        }
        #[cfg(unix)]
        service.recover_runtime_observations().await?;
        Ok(service)
    }

    fn task_response(&self, row: PersistedTask) -> Result<TaskResponse, ErrorBody> {
        let task: Task = serde_json::from_str(&row.aggregate_json).map_err(json_error)?;
        let lease_observation =
            lease_observation::observe(&task, self.clock.read(self.boot_generation));
        Ok(TaskResponse {
            lease_observation,
            project_id: row.project_id,
            objective: row.objective,
            priority: row.priority,
            authority: serde_json::from_str(&row.authority_json).map_err(json_error)?,
            aggregate: Box::new(task),
        })
    }

    async fn attempt_capability_hash(&self, task: &Task) -> Result<Option<String>, ErrorBody> {
        let Some(attempt) = &task.attempt else {
            return Ok(None);
        };
        self.store
            .agent(&attempt.worker.0)
            .await
            .map_err(store_error)?
            .map(|agent| agent.capability_hash)
            .ok_or_else(|| {
                ErrorBody::new(
                    ErrorCode::InvariantViolation,
                    "attempt worker is not registered",
                )
            })
            .map(Some)
    }

    async fn validate_external_actor(&self, actor: &Actor) -> Result<(), ErrorBody> {
        let required_access = match actor.role {
            ActorRole::Operator => return Ok(()),
            ActorRole::System => {
                return Err(ErrorBody::new(
                    ErrorCode::Validation,
                    "system commands are internal to the conductor runtime",
                ));
            }
            ActorRole::Worker => "read_write",
            ActorRole::Verifier => "read_only",
        };
        let agent = self
            .store
            .agent(&actor.id.0)
            .await
            .map_err(store_error)?
            .ok_or_else(|| ErrorBody::new(ErrorCode::Validation, "actor is not registered"))?;
        if agent.access_mode != required_access {
            return Err(ErrorBody::new(
                ErrorCode::Validation,
                "actor role does not match its registered access mode",
            ));
        }
        Ok(())
    }

    async fn validate_claim_worker(&self, command: &Command) -> Result<(), ErrorBody> {
        let CommandKind::Claim { worker, .. } = &command.kind else {
            return Ok(());
        };
        let worker = self
            .store
            .agent(&worker.0)
            .await
            .map_err(store_error)?
            .ok_or_else(|| {
                ErrorBody::new(ErrorCode::Validation, "claim worker is not registered")
            })?;
        if worker.access_mode != "read_write" {
            return Err(ErrorBody::new(
                ErrorCode::Validation,
                "claim worker lacks write access",
            ));
        }
        Ok(())
    }

    /// Requeues every lease whose authoritative deadline and grace have elapsed.
    ///
    /// This is the bounded maintenance operation called by the daemon timer. It
    /// emits the same versioned system command as any other lifecycle mutation;
    /// it never patches task state directly.
    ///
    /// # Errors
    /// Returns an integrity error if persisted aggregates cannot be decoded or
    /// an eligible requeue cannot be committed.
    pub async fn reconcile_expired(&self) -> Result<usize, StoreError> {
        let _guard = self.mutations.lock().await;
        let clock = self.clock.read(self.boot_generation);
        let rows = self.store.tasks(None).await?;
        let mut requeued = 0;
        for row in rows {
            let task: Task = serde_json::from_str(&row.aggregate_json)
                .map_err(|error| StoreError::Integrity(format!("invalid aggregate: {error}")))?;
            let Some(attempt) = task.attempt.as_ref() else {
                continue;
            };
            if attempt.lease.status == LeaseStatus::Ended
                || attempt.lease.boot_generation != clock.boot_generation
            {
                continue;
            }
            let Some(requeue_at) = attempt
                .lease
                .deadline_monotonic_ms
                .checked_add(attempt.lease.recovery_grace_ms)
            else {
                return Err(StoreError::Integrity(format!(
                    "lease deadline overflow for task {}",
                    task.id.0
                )));
            };
            if clock.monotonic_ms < requeue_at {
                continue;
            }
            let unfenced = self.store.has_unfenced_execution(&task.id.0).await?;
            let command = Command {
                actor: Actor {
                    id: ActorId("conductor-reconciler".into()),
                    role: ActorRole::System,
                },
                expected_version: task.version,
                correlation_id: CorrelationId(format!(
                    "reconcile:{}:{}:{}",
                    self.boot_generation, task.id.0, attempt.lease.epoch
                )),
                idempotency_key: IdempotencyKey(format!(
                    "requeue:{}:{}:{}",
                    self.boot_generation, task.id.0, attempt.lease.epoch
                )),
                reason: "lease deadline and recovery grace elapsed".into(),
                kind: if unfenced {
                    CommandKind::Block {
                        blocker: agent_conductor_domain::Blocker {
                            kind: "runtime_fence_unconfirmed".into(),
                            detail: "expired execution has no confirmed write fence".into(),
                            resume_when: "reconcile and fence every prior execution".into(),
                        },
                    }
                } else {
                    CommandKind::RequeueExpired
                },
            };
            self.apply_row(row, command)
                .await
                .map_err(|error| StoreError::Integrity(error.message))?;
            if !unfenced {
                requeued += 1;
            }
        }
        Ok(requeued)
    }

    /// Applies a fake-effect reconciliation contradiction through the normal
    /// versioned task lifecycle. Production adapters are intentionally outside
    /// Phase 1; this is the internal port their typed `BlockTask` directive uses.
    ///
    /// # Errors
    /// Returns a service error when the task is missing or cannot be blocked.
    pub async fn block_effect_contradiction(
        &self,
        task_id: &str,
        effect_token: &str,
        reason: &str,
    ) -> Result<CommandAppliedResponse, ErrorBody> {
        validate_text(&[task_id, effect_token, reason])?;
        let _guard = self.mutations.lock().await;
        let row = self
            .store
            .task(task_id)
            .await
            .map_err(store_error)?
            .ok_or_else(not_found)?;
        let task: Task = serde_json::from_str(&row.aggregate_json).map_err(json_error)?;
        // Replay uses immutable directive input, not the task version read on retry.
        let directive_hash = hash(
            &serde_json::to_string(&("effect-block-v1", task_id, effect_token, reason))
                .map_err(json_error)?,
        );
        self.apply_row_with_hash(
            row,
            Command {
                actor: Actor {
                    id: ActorId("conductor-effect-reconciler".into()),
                    role: ActorRole::System,
                },
                expected_version: task.version,
                correlation_id: CorrelationId(format!("effect-contradiction:{effect_token}")),
                idempotency_key: IdempotencyKey(format!("effect-block:{effect_token}")),
                reason: "fake effect inventory contradiction".into(),
                kind: CommandKind::Block {
                    blocker: agent_conductor_domain::Blocker {
                        kind: "effect_inventory_contradiction".into(),
                        detail: reason.into(),
                        resume_when: "operator resolves the external inventory contradiction"
                            .into(),
                    },
                },
            },
            directive_hash,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_creation(
        &self,
        actor: String,
        key: String,
        request: &Request,
        project: Option<ProjectRegistration>,
        agent: Option<AgentRegistration>,
        new_task: Option<TaskRegistration>,
        outcome: Response,
    ) -> Result<Response, ErrorBody> {
        validate_text(&[&actor, &key])?;
        let request_json = serde_json::to_string(request).map_err(json_error)?;
        let hash = hash(&request_json);
        if let Some(json) = self
            .store
            .command_outcome(&actor, &key, &hash)
            .await
            .map_err(store_error)?
        {
            return serde_json::from_str(&json).map_err(json_error);
        }
        let now = self.clock.read(self.boot_generation).wall_unix_ms;
        let kind = match (&project, &agent, &new_task) {
            (Some(_), _, _) => "project.registered",
            (_, Some(_), _) => "agent.registered",
            _ => "task.created",
        };
        let audit_task_id = new_task.as_ref().map(|task| task.id.clone());
        let batch = CommitBatch {
            actor_id: actor.clone(),
            idempotency_key: key,
            request_hash: hash,
            task: None,
            project,
            agent,
            new_task,
            attempt: None,
            transitions: vec![],
            audit: vec![AuditAppend {
                id: Uuid::now_v7().to_string(),
                task_id: audit_task_id,
                actor_id: actor,
                kind: kind.into(),
                correlation_id: Uuid::now_v7().to_string(),
                payload_json: request_json,
                occurred_at_ms: now,
            }],
            effects: vec![],
            outcome_json: serde_json::to_string(&outcome).map_err(json_error)?,
            committed_at_ms: now,
            expires_at_ms: now.saturating_add(86_400_000),
        };
        match self.store.persist(&batch).await.map_err(store_error)? {
            CommitResult::Applied { .. } => Ok(outcome),
            CommitResult::Replayed { outcome_json } => {
                serde_json::from_str(&outcome_json).map_err(json_error)
            }
        }
    }

    async fn apply_row(
        &self,
        row: PersistedTask,
        command: Command,
    ) -> Result<CommandAppliedResponse, ErrorBody> {
        let request_json =
            serde_json::to_string(&(row.id.as_str(), &command)).map_err(json_error)?;
        self.apply_row_with_hash(row, command, hash(&request_json))
            .await
    }

    // Only internal operations with an immutable input identity may supply this
    // hash. External commands hash their complete versioned payload above.
    async fn validate_execution_fence(
        &self,
        task_id: &str,
        command: &Command,
    ) -> Result<(), ErrorBody> {
        if matches!(
            command.kind,
            CommandKind::Claim { .. } | CommandKind::RequeueExpired
        ) && (self
            .store
            .has_unfenced_execution(task_id)
            .await
            .map_err(store_error)?
            || self
                .store
                .has_pending_auxiliary(task_id)
                .await
                .map_err(store_error)?)
        {
            return Err(ErrorBody::new(
                ErrorCode::Validation,
                "prior execution fencing is unconfirmed",
            ));
        }
        Ok(())
    }

    async fn apply_row_with_hash(
        &self,
        row: PersistedTask,
        command: Command,
        request_hash: String,
    ) -> Result<CommandAppliedResponse, ErrorBody> {
        let request_json =
            serde_json::to_string(&(row.id.as_str(), &command)).map_err(json_error)?;
        if let Some(json) = self
            .store
            .command_outcome(
                &command.actor.id.0,
                &command.idempotency_key.0,
                &request_hash,
            )
            .await
            .map_err(store_error)?
        {
            let mut value: CommandAppliedResponse =
                serde_json::from_str(&json).map_err(json_error)?;
            value.replayed = true;
            return Ok(value);
        }
        let (batch, response) = self
            .prepare_command_batch(row, command, request_hash, request_json)
            .await?;
        match self.store.persist(&batch).await.map_err(store_error)? {
            CommitResult::Applied { .. } => Ok(response),
            CommitResult::Replayed { outcome_json } => {
                let mut value: CommandAppliedResponse =
                    serde_json::from_str(&outcome_json).map_err(json_error)?;
                value.replayed = true;
                Ok(value)
            }
        }
    }
    async fn prepare_command_batch(
        &self,
        row: PersistedTask,
        command: Command,
        request_hash: String,
        request_json: String,
    ) -> Result<(CommitBatch, CommandAppliedResponse), ErrorBody> {
        self.validate_claim_worker(&command).await?;
        self.validate_execution_fence(&row.id, &command).await?;
        let old: Task = serde_json::from_str(&row.aggregate_json).map_err(json_error)?;
        let clock = self.clock.read(self.boot_generation);
        let applied = apply(&old, &command, clock).map_err(rejection_error)?;
        let attempt = attempt_requires_persistence(&old, &applied.task);
        let capability_hash = if attempt {
            self.attempt_capability_hash(&applied.task).await?
        } else {
            None
        };
        let effects = effect_appends(&applied.task, &command, &applied.effects, clock)?;
        let response = CommandAppliedResponse {
            task: TaskResponse {
                lease_observation: None,
                project_id: row.project_id,
                objective: row.objective,
                priority: row.priority,
                authority: serde_json::from_str(&row.authority_json).map_err(json_error)?,
                aggregate: Box::new(applied.task.clone()),
            },
            replayed: false,
            effect_tokens: effects.iter().map(|e| e.token.clone()).collect(),
        };
        let batch = CommitBatch {
            actor_id: command.actor.id.0.clone(),
            idempotency_key: command.idempotency_key.0.clone(),
            request_hash,
            task: Some(TaskMutation {
                task_id: applied.task.id.0.clone(),
                expected_version: to_i64(old.version)?,
                new_version: to_i64(applied.task.version)?,
                state: state(applied.task.state)?,
                aggregate_json: serde_json::to_string(&applied.task).map_err(json_error)?,
                updated_at_ms: clock.wall_unix_ms,
            }),
            project: None,
            agent: None,
            new_task: None,
            attempt: attempt
                .then(|| attempt_mutation(&applied.task, clock, capability_hash.as_deref()))
                .transpose()?
                .flatten(),
            transitions: vec![TaskTransitionAppend {
                id: Uuid::now_v7().to_string(),
                task_id: applied.task.id.0.clone(),
                actor_id: command.actor.id.0.clone(),
                prior_state: state(applied.transition.from)?,
                new_state: state(applied.transition.to)?,
                reason: command.reason.clone(),
                correlation_id: command.correlation_id.0.clone(),
                occurred_at_ms: clock.wall_unix_ms,
            }],
            audit: vec![AuditAppend {
                id: Uuid::now_v7().to_string(),
                task_id: Some(applied.task.id.0.clone()),
                actor_id: command.actor.id.0,
                kind: command_kind(&command.kind).into(),
                correlation_id: command.correlation_id.0,
                payload_json: request_json,
                occurred_at_ms: clock.wall_unix_ms,
            }],
            effects,
            outcome_json: serde_json::to_string(&response).map_err(json_error)?,
            committed_at_ms: clock.wall_unix_ms,
            expires_at_ms: clock.wall_unix_ms.saturating_add(86_400_000),
        };
        Ok((batch, response))
    }
}

impl<C: Clock> ControlService for PersistentControlService<C> {
    #[allow(clippy::too_many_lines)]
    async fn handle(&self, request: Request) -> Result<Response, ErrorBody> {
        match request.clone() {
            Request::ReassignWorkflow {
                task_id,
                expected_version,
                worker_id,
                pin_json,
            } => {
                self.queue_reassignment(&task_id, expected_version, &worker_id, &pin_json)
                    .await
                    .map_err(store_error)?;
                Ok(Response::WorkflowQueued { task_id })
            }
            Request::ContinueWorkflow {
                task_id,
                expected_version,
            } => {
                self.queue_continuation(&task_id, expected_version)
                    .await
                    .map_err(store_error)?;
                Ok(Response::WorkflowQueued { task_id })
            }
            Request::SubmitWorkflow { plan_json } => self.submit_workflow(&plan_json).await,
            Request::ListWorkflows => {
                let mut entries = Vec::new();
                for row in self.store.workflows().await.map_err(store_error)? {
                    let dependencies = self
                        .store
                        .workflow_dependencies(&row.task_id)
                        .await
                        .map_err(store_error)?;
                    let mut value = serde_json::to_value(&row).map_err(json_error)?;
                    value["dependencies"] =
                        serde_json::to_value(dependencies).map_err(json_error)?;
                    value["selected_worker"] = self
                        .store
                        .workflow_assignment(&row.task_id)
                        .await
                        .map_err(store_error)?
                        .map_or(serde_json::Value::Null, |(id, _)| {
                            serde_json::Value::String(id)
                        });
                    entries.push(value);
                }
                Ok(Response::Workflows { entries })
            }
            Request::Health => {
                let v = invariants(&self.store).await?;
                let repository_watcher = self
                    .watcher_status
                    .read()
                    .map_or(WatcherStatus::Failed, |s| *s);
                Ok(Response::Health(HealthResponse {
                    repository_watcher,
                    status: if v.is_empty()
                        && matches!(
                            repository_watcher,
                            WatcherStatus::Disabled | WatcherStatus::Running
                        ) {
                        HealthStatus::Ready
                    } else {
                        HealthStatus::Degraded
                    },
                    boot_generation: self.boot_generation,
                    uptime_millis: u64::try_from(self.started_at.elapsed().as_millis())
                        .unwrap_or(u64::MAX),
                }))
            }
            Request::CheckInvariants => {
                let v = invariants(&self.store).await?;
                Ok(Response::Invariants(InvariantResponse {
                    healthy: v.is_empty(),
                    violations: v,
                }))
            }
            Request::RegisterProject {
                actor_id,
                idempotency_key,
                id,
                path,
            } => {
                validate_text(&[&id, &path])?;
                let now = self.clock.read(self.boot_generation).wall_unix_ms;
                let project = trusted_project_registration(id.clone(), &path, now)?;
                let out = Response::ProjectRegistered { id: id.clone() };
                self.persist_creation(
                    actor_id,
                    idempotency_key,
                    &request,
                    Some(project),
                    None,
                    None,
                    out,
                )
                .await
            }
            Request::RegisterAgent {
                actor_id,
                idempotency_key,
                id,
                adapter_kind,
                protocol_fingerprint,
                capability_hash,
                access_mode,
            } => {
                validate_text(&[&id, &adapter_kind, &protocol_fingerprint, &capability_hash])?;
                if !matches!(access_mode.as_str(), "read_only" | "read_write") {
                    return Err(ErrorBody::new(ErrorCode::Validation, "invalid access_mode"));
                }
                let now = self.clock.read(self.boot_generation).wall_unix_ms;
                let out = Response::AgentRegistered { id: id.clone() };
                self.persist_creation(
                    actor_id,
                    idempotency_key,
                    &request,
                    None,
                    Some(AgentRegistration {
                        id,
                        adapter_kind,
                        protocol_fingerprint,
                        capability_hash,
                        access_mode,
                        created_at_ms: now,
                    }),
                    None,
                    out,
                )
                .await
            }
            Request::CreateTask {
                actor_id,
                idempotency_key,
                id,
                project_id,
                objective,
                priority,
                authority,
                required_checks,
            } => {
                validate_text(&[&id, &project_id, &objective])?;
                let aggregate = Task::new(TaskId(id.clone()), required_checks);
                let now = self.clock.read(self.boot_generation).wall_unix_ms;
                let row = PersistedTask {
                    id: id.clone(),
                    project_id: project_id.clone(),
                    objective: objective.clone(),
                    priority,
                    authority_json: authority.to_string(),
                    aggregate_json: serde_json::to_string(&aggregate).map_err(json_error)?,
                };
                let out = Response::Task(self.task_response(row)?);
                self.persist_creation(
                    actor_id,
                    idempotency_key,
                    &request,
                    None,
                    None,
                    Some(TaskRegistration {
                        id,
                        project_id,
                        objective,
                        state: "backlog".into(),
                        priority,
                        authority_json: authority.to_string(),
                        aggregate_json: serde_json::to_string(&aggregate).map_err(json_error)?,
                        created_at_ms: now,
                    }),
                    out,
                )
                .await
            }
            Request::GetTask { task_id } => {
                let row = self
                    .store
                    .task(&task_id)
                    .await
                    .map_err(store_error)?
                    .ok_or_else(not_found)?;
                Ok(Response::Task(self.task_response(row)?))
            }
            Request::Inventory { resource, after } => self.inventory(resource, after).await,
            Request::ListTaskPage { project_id, after } => {
                if after.as_ref().is_some_and(String::is_empty) {
                    return Err(ErrorBody::new(
                        ErrorCode::Validation,
                        "invalid task page cursor",
                    ));
                }
                let mut rows = self
                    .store
                    .task_page(project_id.as_deref(), after.as_deref())
                    .await
                    .map_err(store_error)?;
                let has_more = rows.len() > 100;
                rows.truncate(100);
                let next_after = if has_more {
                    rows.last().map(|row| row.id.clone())
                } else {
                    None
                };
                let tasks = rows
                    .into_iter()
                    .map(|row| self.task_response(row))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Response::TaskPage { tasks, next_after })
            }
            Request::TaskCheckpoint { task_id } => {
                let view = self.checkpoint_view(&task_id).await.map_err(store_error)?;
                Ok(Response::TaskCheckpoint { view })
            }
            Request::TaskEvidence { task_id, before } => self.evidence(&task_id, before).await,
            Request::TaskHistory { task_id, before } => {
                let before = before
                    .map(|value| {
                        value.parse::<i64>().ok().filter(|v| *v > 0).ok_or_else(|| {
                            ErrorBody::new(ErrorCode::Validation, "invalid history cursor")
                        })
                    })
                    .transpose()?;
                self.store
                    .task(&task_id)
                    .await
                    .map_err(store_error)?
                    .ok_or_else(not_found)?;
                let mut rows = self
                    .store
                    .task_history(&task_id, before)
                    .await
                    .map_err(store_error)?;
                let has_more = rows.len() > 100;
                rows.truncate(100);
                let next_before = if has_more {
                    rows.last().map(|row| row.sequence.to_string())
                } else {
                    None
                };
                Ok(Response::TaskHistory {
                    entries: rows
                        .into_iter()
                        .map(|row| conductor_protocol::TaskHistoryEntry {
                            sequence: row.sequence.to_string(),
                            kind: row.kind,
                            actor_id: row.actor_id,
                            correlation_id: row.correlation_id,
                            occurred_at_ms: row.occurred_at_ms,
                        })
                        .collect(),
                    next_before,
                })
            }
            Request::ListTasks { project_id } => {
                let rows = self
                    .store
                    .tasks(project_id.as_deref())
                    .await
                    .map_err(store_error)?;
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    out.push(self.task_response(row)?);
                }
                Ok(Response::Tasks { tasks: out })
            }
            Request::ApplyCommand { task_id, command } => {
                let _guard = self.mutations.lock().await;
                self.validate_external_actor(&command.actor).await?;
                let row = self
                    .store
                    .task(&task_id)
                    .await
                    .map_err(store_error)?
                    .ok_or_else(not_found)?;
                if row.id != task_id {
                    return Err(ErrorBody::new(
                        ErrorCode::InvariantViolation,
                        "task identity mismatch",
                    ));
                }
                Ok(Response::CommandApplied(
                    self.apply_row(row, command).await?,
                ))
            }
            Request::BindVerificationSource {
                task_id,
                expected_version,
                idempotency_key,
                source,
            } => {
                validate_text(&[&task_id, &idempotency_key, &source.0])?;
                let _guard = self.mutations.lock().await;
                let row = self
                    .store
                    .task(&task_id)
                    .await
                    .map_err(store_error)?
                    .ok_or_else(not_found)?;
                let command = Command {
                    actor: Actor {
                        id: ActorId("conductor-source-binder".into()),
                        role: ActorRole::System,
                    },
                    expected_version,
                    correlation_id: CorrelationId(format!(
                        "bind-source:{task_id}:{idempotency_key}"
                    )),
                    idempotency_key: IdempotencyKey(idempotency_key),
                    reason: "bind conductor-observed verification source".into(),
                    kind: CommandKind::BindVerificationSource { source },
                };
                Ok(Response::CommandApplied(
                    self.apply_row(row, command).await?,
                ))
            }
            Request::SubscribeEvents { subscription } => {
                validate_text(&[
                    &subscription.id,
                    &subscription.agent_id,
                    &subscription.topic_filter,
                ])?;
                let id = subscription.id.clone();
                let now = self.clock.read(self.boot_generation).wall_unix_ms;
                self.store
                    .upsert_event_subscription(&EventSubscription {
                        schema_version: subscription.schema_version,
                        id: subscription.id,
                        agent_id: subscription.agent_id,
                        topic_filter: subscription.topic_filter,
                        project_id: subscription.project_id,
                        path_prefix: subscription.path_prefix,
                        echo_source: subscription.echo_source,
                        created_at_ms: now,
                    })
                    .await
                    .map_err(event_store_error)?;
                Ok(Response::EventSubscribed {
                    subscription_id: id,
                })
            }
            Request::PublishEvent { event } => {
                let _guard = self.mutations.lock().await;
                validate_text(&[
                    &event.id,
                    &event.source_agent_id,
                    &event.source_session_id,
                    &event.correlation_id,
                    &event.topic,
                ])?;
                let payload_json = serde_json::to_string(&event.payload).map_err(json_error)?;
                let clock = self.clock.read(self.boot_generation);
                if let Some(task_id) = &event.task_id {
                    let row = self
                        .store
                        .task(task_id)
                        .await
                        .map_err(store_error)?
                        .ok_or_else(not_found)?;
                    validate_task_event(&row, &event, clock)?;
                } else if event.attempt_id.is_some() || event.lease_epoch.is_some() {
                    return Err(ErrorBody::new(
                        ErrorCode::Validation,
                        "attempt identity requires a task-scoped event",
                    ));
                }
                let idempotency_key = event.idempotency_key.unwrap_or_else(|| {
                    EventSourceIdentity {
                        source_agent_id: &event.source_agent_id,
                        source_session_id: &event.source_session_id,
                        normalized_payload: &payload_json,
                    }
                    .idempotency_key()
                });
                let result = self
                    .store
                    .publish_event(
                        &SemanticEvent {
                            schema_version: event.schema_version,
                            id: event.id,
                            idempotency_key,
                            source_agent_id: event.source_agent_id,
                            source_session_id: event.source_session_id,
                            correlation_id: event.correlation_id,
                            causation_id: event.causation_id,
                            generation: event.generation,
                            topic: event.topic,
                            project_id: event.project_id,
                            task_id: event.task_id,
                            attempt_id: event.attempt_id,
                            lease_epoch: event.lease_epoch,
                            path_scope: event.path_scope,
                            payload_json,
                            created_at_ms: clock.wall_unix_ms,
                        },
                        &EventLimits::default(),
                    )
                    .await
                    .map_err(event_store_error)?;
                let response = match result {
                    PublishResult::Published { event_id, fanout } => EventPublishedResponse {
                        event_id,
                        fanout,
                        duplicate: false,
                    },
                    PublishResult::Duplicate { event_id } => EventPublishedResponse {
                        event_id,
                        fanout: 0,
                        duplicate: true,
                    },
                };
                Ok(Response::EventPublished(response))
            }
            Request::ClaimEvent { agent_id } => {
                validate_text(&[&agent_id])?;
                let now = self.clock.read(self.boot_generation).wall_unix_ms;
                let delivery = self
                    .store
                    .claim_event_delivery(&agent_id, now, &EventLimits::default())
                    .await
                    .map_err(event_store_error)?
                    .map(event_delivery_response)
                    .transpose()?
                    .map(Box::new);
                Ok(Response::EventDelivery { delivery })
            }
            Request::AcknowledgeEvent {
                delivery_id,
                consumer_agent_id,
                claim_token,
                effect,
            } => {
                validate_text(&[
                    &delivery_id,
                    &consumer_agent_id,
                    &claim_token,
                    &effect.namespace,
                    &effect.key,
                ])?;
                self.store
                    .acknowledge_event_with_state(
                        &delivery_id,
                        &consumer_agent_id,
                        &claim_token,
                        &ConsumerStateEffect {
                            namespace: effect.namespace,
                            key: effect.key,
                            expected_version: effect.expected_version,
                            value_json: serde_json::to_string(&effect.value).map_err(json_error)?,
                        },
                        self.clock.read(self.boot_generation).wall_unix_ms,
                    )
                    .await
                    .map_err(event_store_error)?;
                Ok(Response::EventAcknowledged { delivery_id })
            }
        }
    }

    async fn maintain(&self) -> Result<(), ErrorBody> {
        self.invalidate_instruction_tasks()
            .await
            .map_err(store_error)?;
        self.recover_fenced_receipts().await.map_err(store_error)?;
        self.store
            .reconcile_completed_dispatch(
                self.boot_generation,
                self.clock.read(self.boot_generation).wall_unix_ms,
            )
            .await
            .map_err(store_error)?;
        self.reconcile_expired()
            .await
            .map(|_| ())
            .map_err(store_error)
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryControlService {
    generation: u64,
    started: Instant,
    violations: Arc<RwLock<Vec<InvariantViolation>>>,
}
impl InMemoryControlService {
    #[must_use]
    pub fn new(generation: u64) -> Self {
        Self {
            generation,
            started: Instant::now(),
            violations: Arc::new(RwLock::new(vec![])),
        }
    }
    /// Replaces fake violations.
    ///
    /// # Panics
    ///
    /// Panics if another test thread poisoned the lock.
    pub fn replace_violations(&self, v: Vec<InvariantViolation>) {
        *self.violations.write().expect("lock") = v;
    }
}
impl ControlService for InMemoryControlService {
    async fn handle(&self, request: Request) -> Result<Response, ErrorBody> {
        let v = self.violations.read().expect("lock").clone();
        match request {
            Request::Health => Ok(Response::Health(HealthResponse {
                repository_watcher: WatcherStatus::Disabled,
                status: if v.is_empty() {
                    HealthStatus::Ready
                } else {
                    HealthStatus::Degraded
                },
                boot_generation: self.generation,
                uptime_millis: u64::try_from(self.started.elapsed().as_millis())
                    .unwrap_or(u64::MAX),
            })),
            Request::CheckInvariants => Ok(Response::Invariants(InvariantResponse {
                healthy: v.is_empty(),
                violations: v,
            })),
            _ => Err(ErrorBody::new(ErrorCode::Unavailable, "test fake")),
        }
    }
}

async fn invariants(store: &Store) -> Result<Vec<InvariantViolation>, ErrorBody> {
    store
        .check_runtime_invariants()
        .await
        .map_err(store_error)
        .map(|v| v.into_iter().map(map_violation).collect())
}
fn map_violation(v: RuntimeInvariantViolation) -> InvariantViolation {
    InvariantViolation {
        code: v.code,
        summary: v.summary,
        context: serde_json::from_str(&v.context_json)
            .unwrap_or(serde_json::Value::String(v.context_json)),
    }
}
fn attempt_requires_persistence(previous: &Task, next: &Task) -> bool {
    match (previous.attempt.as_ref(), next.attempt.as_ref()) {
        (Some(previous), Some(next)) => {
            previous.id != next.id || previous.worker != next.worker || previous.lease != next.lease
        }
        (None, Some(_)) => true,
        _ => false,
    }
}

fn attempt_mutation(
    task: &Task,
    clock: ClockReading,
    capability_hash: Option<&str>,
) -> Result<Option<AttemptLeaseMutation>, ErrorBody> {
    task.attempt
        .as_ref()
        .map(|a| {
            let remaining = a
                .lease
                .deadline_monotonic_ms
                .saturating_sub(clock.monotonic_ms);
            Ok(AttemptLeaseMutation {
                attempt_id: a.id.0.clone(),
                task_id: task.id.0.clone(),
                worker_id: a.worker.0.clone(),
                lease_epoch: to_i64(a.lease.epoch)?,
                lease_status: match a.lease.status {
                    LeaseStatus::Active => "active",
                    LeaseStatus::RecoveryPending => "recovery_pending",
                    LeaseStatus::Ended => "released",
                }
                .into(),
                boot_generation: to_i64(a.lease.boot_generation)?,
                remaining_ms: to_i64(remaining)?,
                grace_ms: to_i64(a.lease.recovery_grace_ms)?,
                display_expires_at_ms: clock.wall_unix_ms.saturating_add(to_i64(remaining)?),
                last_heartbeat_at_ms: clock.wall_unix_ms,
                instruction_hashes_json: "[]".into(),
                required_checks_json: serde_json::to_string(&task.required_checks)
                    .map_err(json_error)?,
                capability_hash: capability_hash
                    .ok_or_else(|| {
                        ErrorBody::new(
                            ErrorCode::InvariantViolation,
                            "attempt capability identity is unavailable",
                        )
                    })?
                    .to_owned(),
                ended_at_ms: (a.lease.status == LeaseStatus::Ended).then_some(clock.wall_unix_ms),
            })
        })
        .transpose()
}
fn effect_append(
    task: &Task,
    command: &Command,
    effect: &EffectIntentSpec,
    clock: ClockReading,
) -> Result<EffectIntentAppend, ErrorBody> {
    let kind = match effect.kind {
        EffectKind::WorkerSpawn => "worker.spawn",
    };
    let material = format!(
        "v1\0{}\0{}\0{}\0{}\0{kind}",
        task.id.0, effect.attempt_id.0, effect.lease_epoch, command.correlation_id.0
    );
    Ok(EffectIntentAppend {
        token: format!("v1:{:x}", Sha256::digest(material.as_bytes())),
        task_id: Some(task.id.0.clone()),
        attempt_id: Some(effect.attempt_id.0.clone()),
        lease_epoch: Some(to_i64(effect.lease_epoch)?),
        correlation_id: command.correlation_id.0.clone(),
        effect_kind: kind.into(),
        request_json: serde_json::to_string(effect).map_err(json_error)?,
        created_at_ms: clock.wall_unix_ms,
    })
}

fn effect_appends(
    task: &Task,
    command: &Command,
    effects: &[EffectIntentSpec],
    clock: ClockReading,
) -> Result<Vec<EffectIntentAppend>, ErrorBody> {
    effects
        .iter()
        .map(|effect| effect_append(task, command, effect, clock))
        .collect()
}
fn state(v: TaskState) -> Result<String, ErrorBody> {
    serde_json::to_value(v)
        .map_err(json_error)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| ErrorBody::new(ErrorCode::Internal, "invalid state"))
}
fn command_kind(v: &CommandKind) -> &'static str {
    match v {
        CommandKind::MarkReady => "mark_ready",
        CommandKind::Claim { .. } => "claim",
        CommandKind::Start { .. } => "start",
        CommandKind::Heartbeat { .. } => "heartbeat",
        CommandKind::EnterRecovery => "enter_recovery",
        CommandKind::RecoverHeartbeat { .. } => "recover_heartbeat",
        CommandKind::RequeueExpired => "requeue_expired",
        CommandKind::Pause => "pause",
        CommandKind::Block { .. } => "block",
        CommandKind::Resume => "resume",
        CommandKind::BeginVerification { .. } => "begin_verification",
        CommandKind::BindVerificationSource { .. } => "bind_verification_source",
        CommandKind::RecordCheck { .. } => "record_check",
        CommandKind::RecordVerdict { .. } => "record_verdict",
        CommandKind::RejectVerification => "reject_verification",
        CommandKind::Complete { .. } => "complete",
        CommandKind::AddRequiredCheck { .. } => "add_required_check",
        CommandKind::OpenDecision { .. } => "open_decision",
        CommandKind::ResolveDecision { .. } => "resolve_decision",
        CommandKind::Cancel => "cancel",
    }
}
#[allow(clippy::needless_pass_by_value)]
fn rejection_error(e: Rejection) -> ErrorBody {
    let code = match e {
        Rejection::VersionConflict { .. } | Rejection::InvalidLease(_) => ErrorCode::Conflict,
        Rejection::VerificationIncomplete(_) => ErrorCode::InvariantViolation,
        _ => ErrorCode::Validation,
    };
    ErrorBody::new(code, e.to_string())
}
#[allow(clippy::needless_pass_by_value)]
fn store_error(e: StoreError) -> ErrorBody {
    let code = match e {
        StoreError::VersionConflict { .. }
        | StoreError::IdempotencyCollision
        | StoreError::ProjectIdentityConflict
        | StoreError::AttemptIdentityConflict(_) => ErrorCode::Conflict,
        StoreError::Integrity(_) | StoreError::InvalidVersionStep { .. } => {
            ErrorCode::InvariantViolation
        }
        _ => ErrorCode::Internal,
    };
    ErrorBody::new(code, format!("persistent state error: {e}"))
}
#[allow(clippy::needless_pass_by_value)]
fn event_store_error(error: EventStoreError) -> ErrorBody {
    let code = match error {
        EventStoreError::IdempotencyCollision
        | EventStoreError::DeliveryNotLeased
        | EventStoreError::ConsumerStateConflict => ErrorCode::Conflict,
        EventStoreError::UnsupportedSchema(_)
        | EventStoreError::InvalidPayload
        | EventStoreError::PayloadTooLarge
        | EventStoreError::GenerationLimit
        | EventStoreError::InvalidCausalGeneration
        | EventStoreError::AgeLimit
        | EventStoreError::FanoutLimit
        | EventStoreError::RetryLimit
        | EventStoreError::NumericRange => ErrorCode::Validation,
        EventStoreError::Database(_) => ErrorCode::Internal,
    };
    ErrorBody::new(code, format!("event state error: {error}"))
}

fn event_delivery_response(delivery: EventDelivery) -> Result<EventDeliveryResponse, ErrorBody> {
    Ok(EventDeliveryResponse {
        schema_version: delivery.schema_version,
        delivery_id: delivery.delivery_id,
        attempts: delivery.attempts,
        claim_token: delivery.claim_token,
        lease_expires_at_ms: delivery.lease_expires_at_ms,
        event: SemanticEventResponse {
            schema_version: delivery.event.schema_version,
            id: delivery.event.id,
            idempotency_key: delivery.event.idempotency_key,
            source_agent_id: delivery.event.source_agent_id,
            source_session_id: delivery.event.source_session_id,
            correlation_id: delivery.event.correlation_id,
            causation_id: delivery.event.causation_id,
            generation: delivery.event.generation,
            topic: delivery.event.topic,
            project_id: delivery.event.project_id,
            task_id: delivery.event.task_id,
            attempt_id: delivery.event.attempt_id,
            lease_epoch: delivery.event.lease_epoch,
            path_scope: delivery.event.path_scope,
            payload: serde_json::from_str(&delivery.event.payload_json).map_err(json_error)?,
            created_at_ms: delivery.event.created_at_ms,
        },
    })
}

fn validate_task_event(
    row: &PersistedTask,
    event: &conductor_protocol::SemanticEventInput,
    clock: ClockReading,
) -> Result<(), ErrorBody> {
    let task: Task = serde_json::from_str(&row.aggregate_json).map_err(json_error)?;
    let attempt = task
        .attempt
        .as_ref()
        .ok_or_else(|| ErrorBody::new(ErrorCode::Conflict, "task has no active attempt"))?;
    if event.project_id.as_deref() != Some(row.project_id.as_str())
        || event.attempt_id.as_deref() != Some(attempt.id.0.as_str())
        || event.lease_epoch != Some(attempt.lease.epoch)
        || event.source_agent_id != attempt.worker.0
        || !attempt.lease.accepts_worker_mutation(clock)
    {
        return Err(ErrorBody::new(
            ErrorCode::Conflict,
            "task event lease identity is stale or mismatched",
        ));
    }
    Ok(())
}
#[allow(clippy::needless_pass_by_value)]
fn json_error(e: serde_json::Error) -> ErrorBody {
    ErrorBody::new(ErrorCode::Internal, format!("JSON error: {e}"))
}
fn not_found() -> ErrorBody {
    ErrorBody::new(ErrorCode::Validation, "task not found")
}
fn validate_text(v: &[&str]) -> Result<(), ErrorBody> {
    if v.iter().any(|s| s.trim().is_empty()) {
        Err(ErrorBody::new(
            ErrorCode::Validation,
            "required text is empty",
        ))
    } else {
        Ok(())
    }
}

fn trusted_project_registration(
    id: String,
    path: &str,
    created_at_ms: i64,
) -> Result<ProjectRegistration, ErrorBody> {
    let requested = Path::new(path);
    if !requested.is_absolute() {
        return Err(ErrorBody::new(
            ErrorCode::Validation,
            "project path must be absolute",
        ));
    }
    let canonical = fs::canonicalize(requested).map_err(|error| {
        ErrorBody::new(
            ErrorCode::Validation,
            format!("project path cannot be resolved: {error}"),
        )
    })?;
    let metadata = fs::metadata(&canonical).map_err(project_path_error)?;
    if !metadata.is_dir() {
        return Err(ErrorBody::new(
            ErrorCode::Validation,
            "project path must resolve to a directory",
        ));
    }
    Ok(ProjectRegistration {
        id,
        canonical_path: path_text(&canonical)?,
        filesystem_identity: metadata_identity(&metadata)?,
        git_common_dir_identity: git_common_directory(&canonical)?
            .map(|common| fs::metadata(common).map_err(project_path_error))
            .transpose()?
            .map(|common| metadata_identity(&common))
            .transpose()?,
        created_at_ms,
    })
}

fn git_common_directory(project: &Path) -> Result<Option<PathBuf>, ErrorBody> {
    let dot_git = project.join(".git");
    if dot_git.is_dir() {
        return fs::canonicalize(dot_git)
            .map(Some)
            .map_err(project_path_error);
    }
    if !dot_git.is_file() {
        return Ok(None);
    }
    let pointer = fs::read_to_string(&dot_git).map_err(project_path_error)?;
    let git_dir = pointer
        .trim()
        .strip_prefix("gitdir:")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ErrorBody::new(ErrorCode::Validation, "invalid .git pointer"))?;
    let git_dir =
        fs::canonicalize(resolve_from(project, Path::new(git_dir))).map_err(project_path_error)?;
    let common_pointer = git_dir.join("commondir");
    if !common_pointer.is_file() {
        return Ok(Some(git_dir));
    }
    let common = fs::read_to_string(common_pointer).map_err(project_path_error)?;
    let common = common.trim();
    if common.is_empty() {
        return Err(ErrorBody::new(
            ErrorCode::Validation,
            "empty Git common-directory pointer",
        ));
    }
    fs::canonicalize(resolve_from(&git_dir, Path::new(common)))
        .map(Some)
        .map_err(project_path_error)
}

fn resolve_from(base: &Path, value: &Path) -> PathBuf {
    if value.is_absolute() {
        value.to_owned()
    } else {
        base.join(value)
    }
}

#[cfg(unix)]
#[allow(clippy::unnecessary_wraps)]
fn metadata_identity(metadata: &fs::Metadata) -> Result<String, ErrorBody> {
    Ok(format!("unix:{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn metadata_identity(_metadata: &fs::Metadata) -> Result<String, ErrorBody> {
    Err(ErrorBody::new(
        ErrorCode::InvariantViolation,
        "trusted filesystem identity is unsupported on this host",
    ))
}

fn path_text(path: &Path) -> Result<String, ErrorBody> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| ErrorBody::new(ErrorCode::Validation, "project path is not valid UTF-8"))
}

#[allow(clippy::needless_pass_by_value)]
fn project_path_error(error: std::io::Error) -> ErrorBody {
    ErrorBody::new(
        ErrorCode::Validation,
        format!("project identity cannot be resolved: {error}"),
    )
}

fn to_i64(v: u64) -> Result<i64, ErrorBody> {
    i64::try_from(v)
        .map_err(|_| ErrorBody::new(ErrorCode::InvariantViolation, "numeric range exceeded"))
}
fn hash(v: &str) -> String {
    format!("{:x}", Sha256::digest(v.as_bytes()))
}
#[cfg(test)]
mod effect_tests {
    #[cfg(unix)]
    mod broker_journal_tests {
        include!("broker_journal_tests.rs");
    }
    include!("checkpoint_tests.rs");
    include!("reassignment_tests.rs");
    include!("unowned_tests.rs");
    use super::*;
    use agent_conductor_domain::{AttemptId, EffectIntentSpec, EffectKind};
    use agent_conductor_store::{NewAgent, NewProject, NewTask, Store};
    use conductor_protocol::{EventSubscriptionInput, SemanticEventInput};
    use sqlx::{Connection, Executor, SqliteConnection};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    fn command(correlation_id: &str) -> Command {
        Command {
            actor: Actor {
                id: ActorId("worker-1".into()),
                role: ActorRole::Worker,
            },
            expected_version: 1,
            correlation_id: CorrelationId(correlation_id.into()),
            idempotency_key: IdempotencyKey("claim-1".into()),
            reason: "claim".into(),
            kind: CommandKind::Cancel,
        }
    }

    #[test]
    fn effect_token_is_versioned_stable_and_changes_with_identity() {
        let task = Task::new(TaskId("task-1".into()), vec![]);
        let effect = EffectIntentSpec {
            kind: EffectKind::WorkerSpawn,
            attempt_id: AttemptId("attempt-1".into()),
            lease_epoch: 1,
            worker: ActorId("worker-1".into()),
        };
        let clock = ClockReading {
            boot_generation: 7,
            monotonic_ms: 10,
            wall_unix_ms: 20,
        };

        let first = effect_append(&task, &command("corr-1"), &effect, clock).expect("effect");
        let replay = effect_append(&task, &command("corr-1"), &effect, clock).expect("replay");
        assert_eq!(first.token, replay.token);
        assert_eq!(
            first.token,
            "v1:f77d90fe7237d7c900a18c2766422bca4ba18dc1183d1a4812e3a68307313eb6"
        );

        let changed =
            effect_append(&task, &command("corr-2"), &effect, clock).expect("changed effect");
        assert_ne!(first.token, changed.token);
        assert_eq!(first.effect_kind, "worker.spawn");
        assert_eq!(
            serde_json::from_str::<EffectIntentSpec>(&first.request_json).expect("typed request"),
            effect
        );
    }

    #[derive(Clone, Debug)]
    struct FixedClock {
        monotonic_ms: Arc<AtomicU64>,
    }

    impl FixedClock {
        fn new(monotonic_ms: u64) -> Self {
            Self {
                monotonic_ms: Arc::new(AtomicU64::new(monotonic_ms)),
            }
        }

        fn set(&self, monotonic_ms: u64) {
            self.monotonic_ms.store(monotonic_ms, Ordering::SeqCst);
        }
    }

    impl Clock for FixedClock {
        fn read(&self, boot_generation: u64) -> ClockReading {
            ClockReading {
                boot_generation,
                monotonic_ms: self.monotonic_ms.load(Ordering::SeqCst),
                wall_unix_ms: 100,
            }
        }
    }

    #[tokio::test]
    async fn external_actor_roles_are_bound_to_registered_access() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = Store::open(directory.path().join("state.sqlite3"))
            .await
            .expect("store");
        store
            .create_agent(NewAgent {
                id: "worker",
                adapter_kind: "fake",
                protocol_fingerprint: "v1",
                capability_hash: "worker-capability",
                access_mode: "read_write",
                created_at_ms: 1,
            })
            .await
            .expect("worker");
        store
            .create_agent(NewAgent {
                id: "verifier",
                adapter_kind: "fake",
                protocol_fingerprint: "v1",
                capability_hash: "verifier-capability",
                access_mode: "read_only",
                created_at_ms: 1,
            })
            .await
            .expect("verifier");
        let service = PersistentControlService::start_with_clock(store, 100, FixedClock::new(10))
            .await
            .expect("service");

        assert!(
            service
                .validate_external_actor(&Actor {
                    id: ActorId("invented-system".into()),
                    role: ActorRole::System,
                })
                .await
                .is_err()
        );
        assert!(
            service
                .validate_external_actor(&Actor {
                    id: ActorId("invented-verifier".into()),
                    role: ActorRole::Verifier,
                })
                .await
                .is_err()
        );
        assert!(
            service
                .validate_external_actor(&Actor {
                    id: ActorId("worker".into()),
                    role: ActorRole::Verifier,
                })
                .await
                .is_err()
        );
        service
            .validate_external_actor(&Actor {
                id: ActorId("worker".into()),
                role: ActorRole::Worker,
            })
            .await
            .expect("registered write worker");
        service
            .validate_external_actor(&Actor {
                id: ActorId("verifier".into()),
                role: ActorRole::Verifier,
            })
            .await
            .expect("registered read-only verifier");
        let read_only_claim = Command {
            actor: Actor {
                id: ActorId("operator".into()),
                role: ActorRole::Operator,
            },
            expected_version: 0,
            correlation_id: CorrelationId("claim-read-only".into()),
            idempotency_key: IdempotencyKey("claim-read-only".into()),
            reason: "claim read-only agent".into(),
            kind: CommandKind::Claim {
                attempt_id: AttemptId("attempt-read-only".into()),
                worker: ActorId("verifier".into()),
                lease_duration_ms: 10,
                worker_margin_ms: 1,
                recovery_grace_ms: 5,
            },
        };
        assert!(
            service
                .validate_claim_worker(&read_only_claim)
                .await
                .is_err()
        );
    }

    fn lifecycle_command(task: &Task, actor: Actor, kind: CommandKind, key: &str) -> Command {
        Command {
            actor,
            expected_version: task.version,
            correlation_id: CorrelationId(format!("correlation-{key}")),
            idempotency_key: IdempotencyKey(key.into()),
            reason: key.into(),
            kind,
        }
    }

    async fn lifecycle_store() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = Store::open(directory.path().join("state.sqlite3"))
            .await
            .expect("store");
        store
            .create_project(NewProject {
                id: "project",
                canonical_path: "/project",
                filesystem_identity: "test:project",
                git_common_dir_identity: None,
                created_at_ms: 1,
            })
            .await
            .expect("project");
        store
            .create_agent(NewAgent {
                id: "worker",
                adapter_kind: "fake",
                protocol_fingerprint: "v1",
                capability_hash: "worker-capability",
                access_mode: "read_write",
                created_at_ms: 1,
            })
            .await
            .expect("worker");
        let task = Task::new(TaskId("task".into()), vec![]);
        let aggregate = serde_json::to_string(&task).expect("aggregate");
        store
            .create_task(NewTask {
                id: "task",
                project_id: "project",
                objective: "exercise recovery",
                state: "backlog",
                priority: 0,
                authority_json: "{}",
                aggregate_json: &aggregate,
                created_at_ms: 1,
            })
            .await
            .expect("task");
        (directory, store)
    }

    async fn claim_task<C: Clock>(service: &PersistentControlService<C>) -> (Task, String) {
        let row = service
            .store
            .task("task")
            .await
            .expect("task")
            .expect("row");
        let task: Task = serde_json::from_str(&row.aggregate_json).expect("aggregate");
        let ready = service
            .apply_row(
                row,
                lifecycle_command(
                    &task,
                    Actor {
                        id: ActorId("operator".into()),
                        role: ActorRole::Operator,
                    },
                    CommandKind::MarkReady,
                    "ready",
                ),
            )
            .await
            .expect("ready")
            .task
            .aggregate;
        let row = service
            .store
            .task("task")
            .await
            .expect("task")
            .expect("row");
        let response = service
            .apply_row(
                row,
                lifecycle_command(
                    &ready,
                    Actor {
                        id: ActorId("worker".into()),
                        role: ActorRole::Worker,
                    },
                    CommandKind::Claim {
                        attempt_id: AttemptId("attempt".into()),
                        worker: ActorId("worker".into()),
                        lease_duration_ms: 10,
                        worker_margin_ms: 1,
                        recovery_grace_ms: 5,
                    },
                    "claim",
                ),
            )
            .await
            .expect("claim");
        let token = response.effect_tokens.first().expect("spawn token").clone();
        (response.task.aggregate.as_ref().clone(), token)
    }

    #[tokio::test]
    async fn maintenance_requeues_expired_attempt_and_abandons_spawn_intent() {
        let (_directory, store) = lifecycle_store().await;
        let clock = FixedClock::new(0);
        let service = PersistentControlService::start_with_clock(store.clone(), 100, clock.clone())
            .await
            .expect("service");
        let (_, token) = claim_task(&service).await;
        clock.set(15);

        assert_eq!(service.reconcile_expired().await.expect("reconcile"), 1);
        let row = store.task("task").await.expect("task").expect("row");
        let task: Task = serde_json::from_str(&row.aggregate_json).expect("aggregate");
        assert_eq!(task.state, TaskState::Ready);
        assert_eq!(
            store.effect(&token).await.expect("effect").unwrap().status,
            agent_conductor_store::EffectStatus::Abandoned
        );
    }

    #[tokio::test]
    async fn cancellation_abandons_unexecuted_spawn_intent_atomically() {
        let (_directory, store) = lifecycle_store().await;
        let service =
            PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
                .await
                .expect("service");
        let (claimed, token) = claim_task(&service).await;
        let row = store.task("task").await.expect("task").expect("row");
        service
            .apply_row(
                row,
                lifecycle_command(
                    &claimed,
                    Actor {
                        id: ActorId("operator".into()),
                        role: ActorRole::Operator,
                    },
                    CommandKind::Cancel,
                    "cancel",
                ),
            )
            .await
            .expect("cancel");
        assert_eq!(
            store.effect(&token).await.expect("effect").unwrap().status,
            agent_conductor_store::EffectStatus::Abandoned
        );
    }

    #[tokio::test]
    async fn fake_inventory_directive_reaches_audited_task_blocking() {
        use agent_conductor_testkit::{
            CrashPoint, FakeEffectRunner, FakeInventory, ReconcileDirective,
        };
        let (directory, store) = lifecycle_store().await;
        let service =
            PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
                .await
                .expect("service");
        let (_, token) = claim_task(&service).await;
        let inventory_path = directory.path().join("external.sqlite3");
        let inventory = FakeInventory::open(&inventory_path)
            .await
            .expect("inventory");
        let runner = FakeEffectRunner::new(store.clone(), inventory, "fake-runner", 50);
        assert!(matches!(
            runner
                .reconcile(&token, 200, CrashPoint::Never)
                .await
                .expect("settled"),
            ReconcileDirective::Settled(_)
        ));
        let mut external =
            SqliteConnection::connect(&format!("sqlite://{}", inventory_path.display()))
                .await
                .expect("external fixture");
        sqlx::query("UPDATE fake_resources SET request_json = '{}' WHERE token = ?")
            .bind(&token)
            .execute(&mut external)
            .await
            .expect("contradict inventory");
        let ReconcileDirective::BlockTask {
            task_id: Some(task_id),
            reason,
        } = runner
            .reconcile(&token, 300, CrashPoint::Never)
            .await
            .expect("typed directive")
        else {
            panic!("expected task-specific contradiction")
        };
        let response = service
            .block_effect_contradiction(&task_id, &token, &reason)
            .await
            .expect("apply directive");
        assert_eq!(response.task.aggregate.state, TaskState::Blocked);
        let transitions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_transitions WHERE task_id = 'task' AND new_state = 'blocked'")
            .fetch_one(store.readers()).await.expect("transition");
        assert_eq!(transitions, 1);
        assert!(
            service
                .block_effect_contradiction(&task_id, &token, &reason)
                .await
                .expect("retry")
                .replayed
        );
        let transitions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_transitions WHERE task_id = 'task' AND new_state = 'blocked'")
            .fetch_one(store.readers()).await.expect("transition");
        assert_eq!(transitions, 1);
    }

    #[tokio::test]
    async fn effect_contradiction_directive_blocks_the_owning_task() {
        let (_directory, store) = lifecycle_store().await;
        let service = PersistentControlService::start_with_clock(store, 100, FixedClock::new(0))
            .await
            .expect("service");
        let (_claimed, token) = claim_task(&service).await;
        let response = service
            .block_effect_contradiction("task", &token, "done intent has no tagged resource")
            .await
            .expect("internal block directive applies");
        assert_eq!(response.task.aggregate.state, TaskState::Blocked);
        let blocker = response.task.aggregate.blocker.as_ref().expect("blocker");
        assert_eq!(blocker.kind, "effect_inventory_contradiction");
        assert!(blocker.detail.contains("no tagged resource"));
        let replay = service
            .block_effect_contradiction("task", &token, "done intent has no tagged resource")
            .await
            .expect("lost-response retry replays");
        assert!(replay.replayed);
        assert_eq!(
            replay.task.aggregate.version,
            response.task.aggregate.version
        );
        assert_eq!(
            service
                .block_effect_contradiction("task", &token, "different reason")
                .await
                .expect_err("changed directive is a collision")
                .code,
            ErrorCode::Conflict
        );
        let store = service.store.clone();
        drop(service);
        let restarted = PersistentControlService::start_with_clock(store, 200, FixedClock::new(0))
            .await
            .expect("restart");
        let replay = restarted
            .block_effect_contradiction("task", &token, "done intent has no tagged resource")
            .await
            .expect("directive survives restart");
        assert!(replay.replayed);
        assert_eq!(
            replay.task.aggregate.version,
            response.task.aggregate.version
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // Keep the ordered regression scenario together.
    async fn cancellation_serializes_before_task_event_publication() {
        let (directory, store) = lifecycle_store().await;
        store
            .create_agent(NewAgent {
                id: "observer",
                adapter_kind: "fake",
                protocol_fingerprint: "v1",
                capability_hash: "observer-capability",
                access_mode: "read_only",
                created_at_ms: 1,
            })
            .await
            .expect("observer");
        let service =
            PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
                .await
                .expect("service");
        let (claimed, _) = claim_task(&service).await;
        service
            .handle(Request::SubscribeEvents {
                subscription: EventSubscriptionInput {
                    schema_version: 1,
                    id: "observer-task-events".into(),
                    agent_id: "observer".into(),
                    topic_filter: "task.changed".into(),
                    project_id: Some("project".into()),
                    path_prefix: None,
                    echo_source: false,
                },
            })
            .await
            .expect("subscription");

        let database = directory.path().join("state.sqlite3");
        let mut blocker = SqliteConnection::connect(&format!("sqlite://{}", database.display()))
            .await
            .expect("blocking writer connection");
        blocker
            .execute("BEGIN IMMEDIATE")
            .await
            .expect("hold SQLite writer");

        let cancel_service = service.clone();
        let cancel = tokio::spawn(async move {
            cancel_service
                .handle(Request::ApplyCommand {
                    task_id: "task".into(),
                    command: lifecycle_command(
                        &claimed,
                        Actor {
                            id: ActorId("operator".into()),
                            role: ActorRole::Operator,
                        },
                        CommandKind::Cancel,
                        "cancel-before-event",
                    ),
                })
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if service.mutations.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancel holds mutation serialization");

        let publish_service = service.clone();
        let publish = tokio::spawn(async move {
            publish_service
                .handle(Request::PublishEvent {
                    event: SemanticEventInput {
                        schema_version: 1,
                        id: "event-after-cancel".into(),
                        idempotency_key: None,
                        source_agent_id: "worker".into(),
                        source_session_id: "session".into(),
                        correlation_id: "event-after-cancel".into(),
                        causation_id: None,
                        generation: 0,
                        topic: "task.changed".into(),
                        project_id: Some("project".into()),
                        task_id: Some("task".into()),
                        attempt_id: Some("attempt".into()),
                        lease_epoch: Some(1),
                        path_scope: None,
                        payload: serde_json::json!({"state": "running"}),
                    },
                })
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !publish.is_finished(),
            "publish must wait behind cancellation"
        );

        blocker
            .execute("COMMIT")
            .await
            .expect("release SQLite writer");
        let Response::CommandApplied(cancelled) =
            cancel.await.expect("cancel joins").expect("cancel")
        else {
            panic!("unexpected cancel response")
        };
        assert_eq!(cancelled.task.aggregate.state, TaskState::Cancelled);
        let error = publish
            .await
            .expect("publish joins")
            .expect_err("cancelled task rejects worker event");
        assert_eq!(error.code, ErrorCode::Conflict);
        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM semantic_events")
            .fetch_one(store.readers())
            .await
            .expect("event count");
        let delivery_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_deliveries")
            .fetch_one(store.readers())
            .await
            .expect("delivery count");
        assert_eq!((event_count, delivery_count), (0, 0));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // Keep the ordered regression scenario together.
    async fn resumed_task_updates_future_checks_without_mutating_ended_attempt() {
        let (directory, store) = lifecycle_store().await;
        let service =
            PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
                .await
                .expect("service");
        let (claimed, _) = claim_task(&service).await;
        let row = store.task("task").await.expect("task").expect("row");
        let paused = service
            .apply_row(
                row,
                lifecycle_command(
                    &claimed,
                    Actor {
                        id: ActorId("operator".into()),
                        role: ActorRole::Operator,
                    },
                    CommandKind::Pause,
                    "pause-for-new-contract",
                ),
            )
            .await
            .expect("pause")
            .task
            .aggregate;
        let row = store.task("task").await.expect("task").expect("row");
        let ready = service
            .apply_row(
                row,
                lifecycle_command(
                    &paused,
                    Actor {
                        id: ActorId("operator".into()),
                        role: ActorRole::Operator,
                    },
                    CommandKind::Resume,
                    "resume-for-new-contract",
                ),
            )
            .await
            .expect("resume")
            .task
            .aggregate;
        let row = store.task("task").await.expect("task").expect("row");
        let updated = service
            .apply_row(
                row,
                lifecycle_command(
                    &ready,
                    Actor {
                        id: ActorId("operator".into()),
                        role: ActorRole::Operator,
                    },
                    CommandKind::AddRequiredCheck {
                        check: agent_conductor_domain::RequiredCheck {
                            id: agent_conductor_domain::CheckId("new-check".into()),
                            description: "new contract check".into(),
                        },
                    },
                    "add-future-check",
                ),
            )
            .await
            .expect("new check applies without rewriting ended attempt")
            .task
            .aggregate;
        assert_eq!(updated.required_checks.len(), 1);

        let url = format!(
            "sqlite://{}",
            directory.path().join("state.sqlite3").display()
        );
        let mut connection = SqliteConnection::connect(&url)
            .await
            .expect("inspection connection");
        let frozen: String =
            sqlx::query_scalar("SELECT required_checks_json FROM attempts WHERE id = 'attempt'")
                .fetch_one(&mut connection)
                .await
                .expect("ended attempt");
        assert_eq!(frozen, "[]");

        let row = store.task("task").await.expect("task").expect("row");
        let reuse = service
            .apply_row(
                row.clone(),
                lifecycle_command(
                    &updated,
                    Actor {
                        id: ActorId("operator".into()),
                        role: ActorRole::Operator,
                    },
                    CommandKind::Claim {
                        attempt_id: AttemptId("attempt".into()),
                        worker: ActorId("worker".into()),
                        lease_duration_ms: 10,
                        worker_margin_ms: 1,
                        recovery_grace_ms: 5,
                    },
                    "reject-reused-attempt",
                ),
            )
            .await
            .expect_err("ended attempt identity cannot be reused");
        assert_eq!(reuse.code, ErrorCode::Conflict);

        let next_claim = service
            .apply_row(
                row,
                lifecycle_command(
                    &updated,
                    Actor {
                        id: ActorId("operator".into()),
                        role: ActorRole::Operator,
                    },
                    CommandKind::Claim {
                        attempt_id: AttemptId("attempt-next".into()),
                        worker: ActorId("worker".into()),
                        lease_duration_ms: 10,
                        worker_margin_ms: 1,
                        recovery_grace_ms: 5,
                    },
                    "claim-updated-contract",
                ),
            )
            .await
            .expect("next claim");
        assert_eq!(next_claim.task.aggregate.required_checks.len(), 1);
        let next_frozen: String = sqlx::query_scalar(
            "SELECT required_checks_json FROM attempts WHERE id = 'attempt-next'",
        )
        .fetch_one(&mut connection)
        .await
        .expect("next attempt");
        assert!(next_frozen.contains("new-check"));
    }

    #[tokio::test]
    async fn consecutive_restarts_keep_recovery_heartbeat_usable() {
        let (_directory, store) = lifecycle_store().await;
        let first =
            PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
                .await
                .expect("first boot");
        let (claimed, _) = claim_task(&first).await;
        let epoch = claimed.attempt.as_ref().expect("attempt").lease.epoch;
        drop(first);

        let second =
            PersistentControlService::start_with_clock(store.clone(), 200, FixedClock::new(0))
                .await
                .expect("second boot");
        drop(second);
        let third =
            PersistentControlService::start_with_clock(store.clone(), 300, FixedClock::new(0))
                .await
                .expect("third boot");
        let row = store.task("task").await.expect("task").expect("row");
        let recovering: Task = serde_json::from_str(&row.aggregate_json).expect("aggregate");
        let recovered = third
            .apply_row(
                row,
                lifecycle_command(
                    &recovering,
                    Actor {
                        id: ActorId("worker".into()),
                        role: ActorRole::Worker,
                    },
                    CommandKind::RecoverHeartbeat {
                        attempt_id: AttemptId("attempt".into()),
                        lease_epoch: epoch,
                        lease_duration_ms: 20,
                        worker_margin_ms: 2,
                    },
                    "recover-third-boot",
                ),
            )
            .await
            .expect("recovery heartbeat")
            .task
            .aggregate;
        assert_eq!(
            recovered.attempt.as_ref().expect("attempt").lease.status,
            LeaseStatus::Active
        );
        assert_eq!(
            recovered
                .attempt
                .as_ref()
                .expect("attempt")
                .lease
                .boot_generation,
            3
        );
    }

    #[test]
    fn project_intake_rejects_relative_missing_and_file_paths() {
        assert!(trusted_project_registration("p".into(), "relative", 1).is_err());
        assert!(
            trusted_project_registration("p".into(), "/definitely/missing/conductor", 1).is_err()
        );
        let directory = tempfile::tempdir().expect("temporary directory");
        let file = directory.path().join("file");
        fs::write(&file, "not a project").expect("write file");
        assert!(trusted_project_registration("p".into(), &file.to_string_lossy(), 1).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn project_aliases_resolve_to_one_trusted_identity() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let project = directory.path().join("project");
        fs::create_dir(&project).expect("create project");
        let alias = directory.path().join("alias");
        symlink(&project, &alias).expect("create alias");

        let direct = trusted_project_registration("direct".into(), &project.to_string_lossy(), 1)
            .expect("direct identity");
        let aliased = trusted_project_registration("alias".into(), &alias.to_string_lossy(), 1)
            .expect("alias identity");
        assert_eq!(direct.canonical_path, aliased.canonical_path);
        assert_eq!(direct.filesystem_identity, aliased.filesystem_identity);
    }

    #[test]
    fn worktree_pointer_resolves_to_main_git_identity() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let main = directory.path().join("main");
        let common = main.join(".git");
        let worktree_git = common.join("worktrees/w1");
        let worktree = directory.path().join("worktree");
        fs::create_dir_all(&worktree_git).expect("create worktree metadata");
        fs::create_dir(&worktree).expect("create worktree");
        fs::write(worktree_git.join("commondir"), "../..").expect("write common pointer");
        fs::write(
            worktree.join(".git"),
            format!("gitdir: {}", worktree_git.display()),
        )
        .expect("write git pointer");

        let main_identity = trusted_project_registration("main".into(), &main.to_string_lossy(), 1)
            .expect("main identity");
        let worktree_identity =
            trusted_project_registration("worktree".into(), &worktree.to_string_lossy(), 1)
                .expect("worktree identity");
        assert_eq!(
            main_identity.git_common_dir_identity,
            worktree_identity.git_common_dir_identity
        );
    }
    fn runtime_launch(directory: &Path) -> conductor_runtime::RunnerLaunch {
        serde_json::from_value(serde_json::json!({
            "packet": {"contract": {
                "version":1,"task_id":"task","attempt_id":"attempt","lease_epoch":1,"worker_id":"worker",
                "adapter":{"kind":"fake","version":"v1","executable_sha256":"a".repeat(64),"conformance_sha256":"b".repeat(64)},
                "workspace":directory,"starting_head":"c".repeat(40),
                "authority":{"read_roots":[directory],"write_roots":[directory],"network":"denied","allow_workspace_commands":true},
                "instructions":[],"checks":[],"max_runtime_ms":5000
            },"objective":"fixture","checkpoint":null,"untrusted_context":[]},
            "executable":"/bin/true","arguments":[],"environment":{},"output_limit":1024,"heartbeat_timeout_ms":1000
        })).expect("runtime fixture")
    }

    #[tokio::test]
    async fn runtime_ownership_uses_monotonic_deadline_not_sql_active_flag() {
        let (directory, store) = lifecycle_store().await;
        let clock = FixedClock::new(0);
        let service = PersistentControlService::start_with_clock(store.clone(), 100, clock.clone())
            .await
            .expect("service");
        claim_task(&service).await;
        let input = runtime_launch(directory.path());
        service
            .validate_runtime_owner(&input.packet)
            .await
            .expect("current lease");
        let mut wrong = input.packet.clone();
        wrong.contract.worker_id = "other".into();
        assert!(service.validate_runtime_owner(&wrong).await.is_err());
        clock.set(10);
        assert!(service.validate_runtime_owner(&input.packet).await.is_err());
        let state: String =
            sqlx::query_scalar("SELECT status FROM leases WHERE attempt_id = 'attempt'")
                .fetch_one(store.readers())
                .await
                .expect("projection");
        assert_eq!(
            state, "active",
            "expiry does not require maintenance to reject launch"
        );
    }

    #[tokio::test]
    async fn pending_auxiliary_refuses_direct_claim_and_requeue_despite_writer_fence() {
        use sqlx::Connection;
        let (directory, store) = lifecycle_store().await;
        let service =
            PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
                .await
                .unwrap();
        let (task, _) = claim_task(&service).await;
        let mut connection = sqlx::SqliteConnection::connect(&format!(
            "sqlite://{}",
            directory.path().join("state.sqlite3").display()
        ))
        .await
        .unwrap();
        sqlx::query("INSERT INTO executions(effect_token,task_id,attempt_id,lease_epoch,runner_id,packet_json,packet_sha256,state,created_at_ms,updated_at_ms) SELECT token,task_id,attempt_id,lease_epoch,'fixture','{}','hash','fenced',0,0 FROM effect_intents").execute(&mut connection).await.unwrap();
        sqlx::query("INSERT INTO auxiliary_invocations (invocation_id,task_id,execution_token,preparation_json,boot_generation,state,created_at_ms,settled_at_ms) SELECT 'aux',task_id,effect_token,'{}',1,'pending',0,NULL FROM executions").execute(&mut connection).await.unwrap();
        assert!(!store.has_unfenced_execution("task").await.unwrap());
        for kind in [
            CommandKind::RequeueExpired,
            CommandKind::Claim {
                attempt_id: AttemptId("next".into()),
                worker: ActorId("worker".into()),
                lease_duration_ms: 10,
                worker_margin_ms: 1,
                recovery_grace_ms: 5,
            },
        ] {
            let command = lifecycle_command(
                &task,
                Actor {
                    id: ActorId("operator".into()),
                    role: ActorRole::Operator,
                },
                kind,
                "blocked-aux",
            );
            assert!(
                service
                    .validate_execution_fence("task", &command)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn failed_spawn_retains_expected_identity_without_ack_or_fence() {
        use agent_conductor_store::{EffectAudit, ExecutionState};
        let (directory, store) = lifecycle_store().await;
        let service =
            PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
                .await
                .unwrap();
        let (_, token) = claim_task(&service).await;
        store
            .claim_effect(
                &token,
                "runner",
                10000,
                EffectAudit {
                    id: "claim",
                    actor_id: "runtime",
                    correlation_id: &token,
                    now_ms: 100,
                },
            )
            .await
            .unwrap();
        let launch = runtime_launch(directory.path());
        let missing = directory.path().join("missing-wrapper");
        assert!(
            service
                .launch_controlled_execution(&missing, &launch, &token, "runner")
                .await
                .is_err()
        );
        let record = store.execution(&token).await.unwrap().unwrap();
        assert_eq!(record.state, ExecutionState::Prepared);
        assert!(record.preparation.expected_wrapper_identity.is_some());
        assert!(record.wrapper_identity.is_none());
        assert!(record.process.is_none());
        assert!(store.has_unfenced_execution("task").await.unwrap());
        assert!(
            service
                .launch_controlled_execution(&missing, &launch, &token, "runner")
                .await
                .is_err()
        );
        assert_eq!(store.execution(&token).await.unwrap().unwrap(), record);
    }

    fn assert_launch_binding(record: &agent_conductor_store::ExecutionRecord) {
        assert!(record.preparation.expected_wrapper_identity.is_some());
        assert_eq!(
            record.preparation.expected_wrapper_identity,
            record.wrapper_identity
        );
        assert!(
            record
                .process
                .as_ref()
                .is_some_and(|process| process.pid > 0)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn controlled_launch_records_ack_refuses_replay_and_expired_renewal() {
        use agent_conductor_store::{EffectAudit, ExecutionState};
        use conductor_runtime::{PacketAcknowledgement, RunnerEvent};
        use std::os::unix::fs::PermissionsExt;
        let (directory, store) = lifecycle_store().await;
        let clock = FixedClock::new(0);
        let service = PersistentControlService::start_with_clock(store.clone(), 100, clock.clone())
            .await
            .expect("service");
        let (_, token) = claim_task(&service).await;
        store
            .claim_effect(
                &token,
                "runner",
                10000,
                EffectAudit {
                    id: "claim-runtime",
                    actor_id: "runtime",
                    correlation_id: &token,
                    now_ms: 100,
                },
            )
            .await
            .expect("effect claim");
        let input = runtime_launch(directory.path());
        let ready = serde_json::to_string(&RunnerEvent::Ready {
            acknowledgement: PacketAcknowledgement {
                version: 1,
                attempt_id: "attempt".into(),
                lease_epoch: 1,
                packet_sha256: input.packet.sha256().expect("hash"),
                instruction_sha256: vec![],
            },
        })
        .expect("ready");
        let wrapper = directory.path().join("wrapper.sh");
        // A controlled protocol peer: actual process-group watchdog behavior is
        // independently exercised by conductor-runtime's real wrapper tests.
        std::fs::write(&wrapper, format!("#!/bin/sh\nread -r launch\nprintf '%s\\n' '{ready}'\nread -r release\nread -r next\nprintf '%s\\n' '{{\"type\":\"stopped\",\"reason\":\"controlled fixture\"}}'\n")).expect("fixture");
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700))
            .expect("executable");
        let (record, mut connection) = service
            .launch_controlled_execution(&wrapper, &input, &token, "runner")
            .await
            .expect("launch");
        assert_eq!(record.state, ExecutionState::Started);
        assert_launch_binding(&record);
        assert_eq!(
            store.execution(&token).await.expect("read"),
            Some(record.clone())
        );
        assert!(
            service
                .launch_controlled_execution(&wrapper, &input, &token, "runner")
                .await
                .is_err()
        );
        clock.set(10);
        assert!(
            service
                .renew_controlled_execution(
                    &token,
                    record.wrapper_identity.as_deref().expect("identity"),
                    &mut connection
                )
                .await
                .is_err()
        );
        let (stopped, event) = service
            .stop_controlled_execution(
                &token,
                record.wrapper_identity.as_deref().expect("identity"),
                connection,
            )
            .await
            .expect("record stop");
        assert_eq!(stopped.state, ExecutionState::Blocked);
        assert!(matches!(event, Some(RunnerEvent::Stopped { .. })));
        assert_eq!(
            store.execution(&token).await.expect("persisted"),
            Some(stopped)
        );
        clock.set(15);
        assert_eq!(service.reconcile_expired().await.expect("reconcile"), 0);
        let row = store.task("task").await.expect("task").expect("row");
        let blocked: Task = serde_json::from_str(&row.aggregate_json).expect("aggregate");
        assert_eq!(blocked.state, TaskState::Blocked);
        assert_eq!(
            blocked.blocker.expect("blocker").kind,
            "runtime_fence_unconfirmed"
        );
        assert!(
            store
                .has_unfenced_execution("task")
                .await
                .expect("fence status")
        );
    }
    #[tokio::test]
    // Keep persisted invalidation, operator precedence and restart in one scenario.
    #[allow(clippy::too_many_lines)]
    async fn instruction_invalidation_is_durable_and_preserves_operator_stop() {
        use agent_conductor_store::{EffectAudit, ExecutionPreparation};
        use conductor_runtime::InstructionPin;
        for operator_stop in [None, Some(CommandKind::Pause), Some(CommandKind::Cancel)] {
            let (directory, store) = lifecycle_store().await;
            let service =
                PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
                    .await
                    .unwrap();
            let (claimed, token) = claim_task(&service).await;
            store
                .claim_effect(
                    &token,
                    "runner",
                    10000,
                    EffectAudit {
                        id: "claim",
                        actor_id: "runtime",
                        correlation_id: &token,
                        now_ms: 100,
                    },
                )
                .await
                .unwrap();
            let root = directory.path().canonicalize().unwrap();
            let workspace = root.join("work");
            fs::create_dir(&workspace).unwrap();
            fs::write(workspace.join("retained"), "unfinished work").unwrap();
            let source = root.join("outer-policy.md");
            fs::write(&source, "approved").unwrap();
            let mut input = runtime_launch(&workspace);
            input.packet.contract.instructions = vec![InstructionPin {
                path: source.clone(),
                sha256: hash("approved"),
            }];
            input
                .packet
                .contract
                .authority
                .read_roots
                .push(source.clone());
            store
                .prepare_execution(
                    &ExecutionPreparation {
                        expected_wrapper_identity: None,
                        effect_token: token.clone(),
                        task_id: "task".into(),
                        attempt_id: "attempt".into(),
                        lease_epoch: 1,
                        runner_id: "runner".into(),
                        packet_json: serde_json::to_string(&input.packet).unwrap(),
                        packet_sha256: input.packet.sha256().unwrap(),
                    },
                    EffectAudit {
                        id: "prepare",
                        actor_id: "runtime",
                        correlation_id: &token,
                        now_ms: 100,
                    },
                )
                .await
                .unwrap();
            service.invalidate_instruction_tasks().await.unwrap();
            assert_eq!(
                service.workflow_task("task").await.unwrap().version,
                claimed.version
            );
            if let Some(kind) = operator_stop.clone() {
                let row = store.task("task").await.unwrap().unwrap();
                service
                    .apply_row(
                        row,
                        lifecycle_command(
                            &claimed,
                            Actor {
                                id: ActorId("operator".into()),
                                role: ActorRole::Operator,
                            },
                            kind,
                            "operator-stop",
                        ),
                    )
                    .await
                    .unwrap();
            }
            fs::write(&source, "changed").unwrap();
            service.invalidate_instruction_tasks().await.unwrap();
            let task = service.workflow_task("task").await.unwrap();
            let expected = match operator_stop {
                None => TaskState::Blocked,
                Some(CommandKind::Pause) => TaskState::Paused,
                _ => TaskState::Cancelled,
            };
            assert_eq!(task.state, expected);
            if expected == TaskState::Blocked {
                assert_eq!(task.blocker.as_ref().unwrap().kind, "instruction_changed");
            }
            service.invalidate_instruction_tasks().await.unwrap();
            assert_eq!(
                service.workflow_task("task").await.unwrap().version,
                task.version
            );
            assert_eq!(
                fs::read(workspace.join("retained")).unwrap(),
                b"unfinished work"
            );
            drop(service);
            let restarted =
                PersistentControlService::start_with_clock(store, 101, FixedClock::new(0))
                    .await
                    .unwrap();
            assert_eq!(
                restarted.workflow_task("task").await.unwrap().state,
                expected
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn native_terminal_observation_binds_output_and_blocks_invalid_results() {
        use agent_conductor_store::{EffectAudit, ExecutionState};
        use conductor_runtime::{AdapterKind, PacketAcknowledgement, RunnerEvent};
        use std::os::unix::fs::PermissionsExt;
        for (valid, replace_packet, fail_publication, stopped, watch_final) in [
            (true, false, false, false, true),
            (true, false, false, false, false),
            (false, false, false, false, false),
            (true, true, false, false, false),
            (true, false, true, false, false),
            (true, false, false, true, false),
            (false, false, false, true, false),
        ] {
            let (directory, store) = lifecycle_store().await;
            let service =
                PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
                    .await
                    .expect("service");
            let (_, token) = claim_task(&service).await;
            store
                .claim_effect(
                    &token,
                    "runner",
                    10000,
                    EffectAudit {
                        id: "native-claim",
                        actor_id: "runtime",
                        correlation_id: &token,
                        now_ms: 100,
                    },
                )
                .await
                .expect("claim");
            let row = store.task("task").await.unwrap().unwrap();
            let task = serde_json::from_str(&row.aggregate_json).unwrap();
            service
                .apply_row(
                    row,
                    lifecycle_command(
                        &task,
                        Actor {
                            id: ActorId("worker".into()),
                            role: ActorRole::Worker,
                        },
                        CommandKind::Start {
                            attempt_id: AttemptId("attempt".into()),
                            lease_epoch: 1,
                        },
                        "start-native",
                    ),
                )
                .await
                .unwrap();
            let workspace = directory.path().join("work");
            fs::create_dir(&workspace).unwrap();
            fs::write(workspace.join("result.txt"), "candidate source").unwrap();
            let workspace = workspace.canonicalize().unwrap();
            let mut input = runtime_launch(&workspace);
            input.packet.contract.authority.allow_workspace_commands = false;
            input.packet.contract.adapter.kind = AdapterKind::Claude;
            let ready = serde_json::to_string(&RunnerEvent::Ready {
                acknowledgement: PacketAcknowledgement {
                    version: 1,
                    attempt_id: "attempt".into(),
                    lease_epoch: 1,
                    packet_sha256: input.packet.sha256().expect("hash"),
                    instruction_sha256: vec![],
                },
            })
            .expect("ready");
            let stdout = if valid {
                b"{\"type\":\"system\",\"subtype\":\"init\",\"tools\":[\"Read\",\"Write\",\"Edit\",\"Glob\",\"Grep\"]}\n{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"candidate only\"}\n".to_vec()
            } else {
                b"truncated".to_vec()
            };
            let event = if stopped {
                RunnerEvent::Stopped {
                    reason: "cancelled".into(),
                    stdout,
                }
            } else {
                RunnerEvent::Finished {
                    exit_code: Some(0),
                    stdout,
                    stderr: vec![],
                }
            };
            let finished = serde_json::to_string(&event).expect("terminal");
            let wrapper = directory.path().join("peer.sh");
            // Tests service lifecycle semantics, not vendor identity or sandboxing.
            let closed_input = directory.path().join("input-closed");
            fs::write(&wrapper, format!("#!/bin/sh\nread -r launch\nprintf '%s\\n' '{ready}'\nread -r release\nexec 0<&-\n: > '{}'\nsleep 0.2\nprintf '%s\\n' '{finished}'\n", closed_input.display())).expect("peer");
            fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o700)).expect("mode");
            let (record, connection) = service
                .launch_execution(&wrapper, &input, &token, "runner")
                .await
                .expect("release");
            let project = store.task("task").await.unwrap().unwrap().project_id;
            let mut owned_state = OwnedWatchState::default();
            assert_eq!(
                service
                    .poll_owned_workspaces(
                        &project,
                        &mut owned_state,
                        std::time::Duration::from_secs(10)
                    )
                    .await
                    .unwrap(),
                0
            );
            store
                .upsert_event_subscription(&EventSubscription {
                    schema_version: 1,
                    id: "owned-test".into(),
                    agent_id: "worker".into(),
                    topic_filter: "file.*".into(),
                    project_id: Some(project.clone()),
                    path_prefix: None,
                    echo_source: true,
                    created_at_ms: 100,
                })
                .await
                .unwrap();
            fs::write(workspace.join("observed.txt"), "private observed output").unwrap();
            assert_eq!(
                service
                    .poll_owned_workspaces(
                        &project,
                        &mut owned_state,
                        std::time::Duration::from_secs(10)
                    )
                    .await
                    .unwrap(),
                0
            );
            assert_eq!(
                service
                    .poll_owned_workspaces(
                        &project,
                        &mut owned_state,
                        std::time::Duration::from_secs(11)
                    )
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(
                service
                    .poll_owned_workspaces(
                        &project,
                        &mut owned_state,
                        std::time::Duration::from_secs(11)
                    )
                    .await
                    .unwrap(),
                0
            );
            let delivery = store
                .claim_event_delivery("worker", 100, &EventLimits::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(delivery.event.source_agent_id, "worker");
            assert_eq!(delivery.event.task_id.as_deref(), Some("task"));
            assert_eq!(delivery.event.attempt_id.as_deref(), Some("attempt"));
            assert_eq!(delivery.event.lease_epoch, Some(1));
            assert_eq!(delivery.event.correlation_id, token);
            assert_eq!(delivery.event.generation, 0);
            assert!(
                !delivery
                    .event
                    .payload_json
                    .contains("private observed output")
            );
            fs::remove_file(workspace.join("observed.txt")).unwrap();
            assert_eq!(
                service
                    .poll_owned_workspaces(
                        &project,
                        &mut owned_state,
                        std::time::Duration::from_secs(12)
                    )
                    .await
                    .unwrap(),
                0
            );
            assert_eq!(
                service
                    .poll_owned_workspaces(
                        &project,
                        &mut owned_state,
                        std::time::Duration::from_secs(13)
                    )
                    .await
                    .unwrap(),
                1
            );
            if valid && !replace_packet && !fail_publication && !stopped {
                if watch_final {
                    store
                        .create_agent(agent_conductor_store::NewAgent {
                            id: "fixture-observer",
                            adapter_kind: "observer",
                            protocol_fingerprint: "fixture",
                            capability_hash: "fixture",
                            access_mode: "read_only",
                            created_at_ms: 100,
                        })
                        .await
                        .unwrap();
                    service.configure_repository_observation(&[RepositoryWatch {
                        id: "terminal-watch".into(),
                        project_id: project.clone(),
                        source_agent_id: "fixture-observer".into(),
                        root: workspace.clone(),
                    }]);
                }
                // Simulate a final burst after the last periodic observation.
                for index in 0..300 {
                    fs::write(
                        workspace.join(format!("terminal-{index:04}.txt")),
                        "terminal bytes",
                    )
                    .unwrap();
                }
            }
            // Force a failed renewal while the matching terminal frame is still
            // in transit. Previously this discarded a successful native result.
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while !closed_input.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("peer closed its input");
            let mut packet = input.packet.clone();
            if replace_packet {
                packet.objective = "different task text".into();
            }
            let result = service
                .await_native_execution(
                    &token,
                    record.wrapper_identity.as_deref().expect("identity"),
                    &packet,
                    connection,
                )
                .await;
            let persisted = store
                .execution(&token)
                .await
                .expect("read")
                .expect("record");
            if replace_packet {
                assert!(result.is_err());
                assert_eq!(persisted.state, ExecutionState::Started);
                service.clock.set(100_000);
                fs::write(workspace.join("expired.txt"), "unowned after lease expiry").unwrap();
                assert_eq!(
                    service
                        .poll_owned_workspaces(
                            &project,
                            &mut owned_state,
                            std::time::Duration::from_secs(14)
                        )
                        .await
                        .unwrap(),
                    0
                );
                store
                    .create_agent(agent_conductor_store::NewAgent {
                        id: "observer",
                        adapter_kind: "observer",
                        protocol_fingerprint: "fixture",
                        capability_hash: "fixture",
                        access_mode: "read_only",
                        created_at_ms: 100,
                    })
                    .await
                    .unwrap();
                store
                    .create_agent(agent_conductor_store::NewAgent {
                        id: "external-consumer",
                        adapter_kind: "observer",
                        protocol_fingerprint: "fixture",
                        capability_hash: "fixture",
                        access_mode: "read_only",
                        created_at_ms: 100,
                    })
                    .await
                    .unwrap();
                store
                    .upsert_event_subscription(&EventSubscription {
                        schema_version: 1,
                        id: "unowned-test".into(),
                        agent_id: "external-consumer".into(),
                        topic_filter: "workspace.external.*".into(),
                        project_id: Some(project.clone()),
                        path_prefix: None,
                        echo_source: false,
                        created_at_ms: 100,
                    })
                    .await
                    .unwrap();
                service.configure_repository_observation(&[RepositoryWatch {
                    id: "external-scope".into(),
                    project_id: project.clone(),
                    source_agent_id: "observer".into(),
                    root: workspace.clone(),
                }]);
                assert_eq!(
                    service
                        .poll_owned_workspaces(
                            &project,
                            &mut owned_state,
                            std::time::Duration::from_secs(15)
                        )
                        .await
                        .unwrap(),
                    0
                );
                assert_eq!(
                    service
                        .poll_owned_workspaces(
                            &project,
                            &mut owned_state,
                            std::time::Duration::from_secs(16)
                        )
                        .await
                        .unwrap(),
                    1,
                    "expired ownership changes must be observable without claiming the old lease"
                );
                let external = store
                    .claim_event_delivery("external-consumer", 100_000, &EventLimits::default())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(external.event.source_agent_id, "observer");
                assert_eq!(external.event.topic, "workspace.external.created");
                assert_eq!(external.event.path_scope.as_deref(), Some("expired.txt"));
                assert!(
                    external.event.attempt_id.is_none() && external.event.lease_epoch.is_none()
                );
                let payload: serde_json::Value =
                    serde_json::from_str(&external.event.payload_json).unwrap();
                assert_eq!(payload["attribution"], "external_workspace");
                assert!(
                    !external
                        .event
                        .payload_json
                        .contains("unowned after lease expiry")
                );
            } else if stopped && valid {
                assert!(result.is_err(), "stop is never a successful candidate");
                assert_eq!(persisted.state, ExecutionState::Fenced);
                let task: Task = serde_json::from_str(
                    &store.task("task").await.unwrap().unwrap().aggregate_json,
                )
                .unwrap();
                assert!(
                    service
                        .continuation_packet("task", task.version)
                        .await
                        .is_ok()
                );
                exercise_retained_observation(&service, &store, &workspace, &project, &token).await;
            } else if valid {
                let (observed, mut candidate) = result.expect("candidate");
                assert_eq!(candidate.text, "candidate only");
                assert_eq!(observed.state, ExecutionState::Exited);
                assert_eq!(persisted, observed);
                candidate.file_only_tools = false;
                assert!(
                    service
                        .finalize_native_source(
                            &token,
                            &candidate,
                            &directory.path().join("refused"),
                            &[]
                        )
                        .await
                        .is_err()
                );
                assert!(!directory.path().join("refused").exists());
                assert!(store.has_unfenced_execution("task").await.unwrap());
                let current: Task = serde_json::from_str(
                    &store.task("task").await.unwrap().unwrap().aggregate_json,
                )
                .unwrap();
                assert!(
                    service
                        .continuation_packet("task", current.version)
                        .await
                        .is_err(),
                    "unfenced execution cannot continue"
                );
                candidate.file_only_tools = true;
                if fail_publication {
                    let mut connection = SqliteConnection::connect(&format!(
                        "sqlite://{}",
                        directory.path().join("state.sqlite3").display()
                    ))
                    .await
                    .unwrap();
                    connection.execute("CREATE TRIGGER refuse_source_binding BEFORE INSERT ON audit_events WHEN NEW.kind = 'bind_verification_source' BEGIN SELECT RAISE(ABORT, 'injected late publication failure'); END").await.unwrap();
                    let before = store.task("task").await.unwrap().unwrap();
                    assert!(
                        service
                            .finalize_native_source(
                                &token,
                                &candidate,
                                &directory.path().join("failed-final"),
                                &[]
                            )
                            .await
                            .is_err()
                    );
                    let after = store.task("task").await.unwrap().unwrap();
                    assert_eq!(
                        before.aggregate_json, after.aggregate_json,
                        "begin transition must roll back with bind failure"
                    );
                    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE kind IN ('begin_verification', 'bind_verification_source')").fetch_one(&mut connection).await.unwrap();
                    assert_eq!(count, 0, "audit must roll back too");
                    let replay_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM command_idempotency WHERE idempotency_key LIKE 'finalize:%'").fetch_one(&mut connection).await.unwrap();
                    assert_eq!(replay_count, 0, "no phantom replay receipt");
                    assert_eq!(
                        store.execution(&token).await.unwrap().unwrap().state,
                        ExecutionState::Fenced
                    );
                    assert!(
                        directory
                            .path()
                            .join("failed-final/tree/result.txt")
                            .exists(),
                        "capture preserved for recovery"
                    );
                    continue;
                }
                let captured = service
                    .finalize_native_source(
                        &token,
                        &candidate,
                        &directory.path().join("final"),
                        &[],
                    )
                    .await
                    .expect("capture and bind");
                assert_eq!(captured.entries.len(), 301);
                let mut database = SqliteConnection::connect(&format!(
                    "sqlite://{}",
                    directory.path().join("state.sqlite3").display()
                ))
                .await
                .unwrap();
                let deliveries: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_deliveries d JOIN semantic_events e ON e.id=d.event_id WHERE e.path_scope LIKE 'terminal-%' AND e.attempt_id='attempt' AND e.lease_epoch=1").fetch_one(&mut database).await.unwrap();
                assert_eq!(deliveries, if watch_final { 300 } else { 0 });
                assert_eq!(
                    service
                        .poll_owned_workspaces(
                            &project,
                            &mut owned_state,
                            std::time::Duration::from_secs(14)
                        )
                        .await
                        .unwrap(),
                    0
                );
                let row = store.task("task").await.unwrap().unwrap();
                let task: Task = serde_json::from_str(&row.aggregate_json).unwrap();
                assert_eq!(task.state, TaskState::Verifying);
                assert!(
                    service
                        .continuation_packet("task", task.version)
                        .await
                        .is_ok()
                );
                assert!(
                    service
                        .continuation_packet("task", task.version - 1)
                        .await
                        .is_err()
                );
                assert_eq!(
                    task.verification.unwrap().source.unwrap().0,
                    format!("snapshot:{}", captured.sha256().unwrap())
                );
            } else {
                assert!(result.is_err());
                assert_eq!(persisted.state, ExecutionState::Blocked);
                exercise_retained_observation(&service, &store, &workspace, &project, &token).await;
            }
            assert_eq!(
                store.has_unfenced_execution("task").await.unwrap(),
                !valid || replace_packet
            );
            let row = store.task("task").await.expect("task").expect("row");
            let task: Task = serde_json::from_str(&row.aggregate_json).expect("aggregate");
            assert_ne!(task.state, TaskState::Completed);
        }
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    // Keep the complete persisted lifecycle and cancellation race visible together.
    #[allow(clippy::too_many_lines)]
    async fn runtime_checks_publish_bound_evidence_but_reject_concurrent_cancellation() {
        use agent_conductor_domain::{CheckId, RequiredCheck, SourceIdentity};
        use agent_conductor_store::{EffectAudit, ExecutionPreparation, ExecutionUpdate};
        use conductor_runtime::{CheckDefinition, CheckOutcome, snapshot_paths};
        use sqlx::Connection;
        for cancel in [false, true] {
            let (directory, store) = lifecycle_store().await;
            let service =
                PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
                    .await
                    .expect("service");
            let operator = Actor {
                id: ActorId("operator".into()),
                role: ActorRole::Operator,
            };
            let worker = Actor {
                id: ActorId("worker".into()),
                role: ActorRole::Worker,
            };
            let row = store.task("task").await.unwrap().unwrap();
            let task: Task = serde_json::from_str(&row.aggregate_json).unwrap();
            service
                .apply_row(
                    row,
                    lifecycle_command(
                        &task,
                        operator.clone(),
                        CommandKind::AddRequiredCheck {
                            check: RequiredCheck {
                                id: CheckId("test".into()),
                                description: "fixture test".into(),
                            },
                        },
                        "require-test",
                    ),
                )
                .await
                .unwrap();
            let (_, token) = claim_task(&service).await;
            let audit = |id| EffectAudit {
                id,
                actor_id: "runtime",
                correlation_id: &token,
                now_ms: 100,
            };
            store
                .claim_effect(&token, "runner", 10000, audit("claim-runtime"))
                .await
                .unwrap();
            let source = directory.path().join("source");
            fs::create_dir(&source).unwrap();
            fs::write(source.join("input"), "original").unwrap();
            let inventory = vec!["input".into()];
            let digest = snapshot_paths(&source, &directory.path().join("capture"), &inventory)
                .unwrap()
                .sha256()
                .unwrap();
            let mut input = runtime_launch(directory.path());
            input.packet.contract.checks = vec![CheckDefinition {
                id: "test".into(),
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "echo started > marker; sleep 0.3; test \"$(cat input)\" = original".into(),
                ],
                timeout_ms: 5000,
            }];
            let record = store
                .prepare_execution(
                    &ExecutionPreparation {
                        expected_wrapper_identity: None,
                        effect_token: token.clone(),
                        task_id: "task".into(),
                        attempt_id: "attempt".into(),
                        lease_epoch: 1,
                        runner_id: "runner".into(),
                        packet_json: serde_json::to_string(&input.packet).unwrap(),
                        packet_sha256: input.packet.sha256().unwrap(),
                    },
                    audit("prepare-runtime"),
                )
                .await
                .unwrap();
            // Fixture has no actual writer; production requires its own fence evidence.
            store
                .update_execution(
                    &token,
                    record.revision,
                    "fence-fixture",
                    &ExecutionUpdate::Fenced {
                        reason: "no writer launched by fixture".into(),
                    },
                    audit("fence-runtime"),
                )
                .await
                .unwrap();
            for (actor, kind, key) in [
                (
                    worker.clone(),
                    CommandKind::Start {
                        attempt_id: AttemptId("attempt".into()),
                        lease_epoch: 1,
                    },
                    "start",
                ),
                (
                    worker,
                    CommandKind::BeginVerification {
                        attempt_id: AttemptId("attempt".into()),
                        lease_epoch: 1,
                    },
                    "verify",
                ),
                (
                    Actor {
                        id: ActorId("runtime".into()),
                        role: ActorRole::System,
                    },
                    CommandKind::BindVerificationSource {
                        source: SourceIdentity(format!("snapshot:{digest}")),
                    },
                    "bind",
                ),
            ] {
                let row = store.task("task").await.unwrap().unwrap();
                let task = serde_json::from_str(&row.aggregate_json).unwrap();
                service
                    .apply_row(row, lifecycle_command(&task, actor, kind, key))
                    .await
                    .unwrap();
            }
            // This production-path test requires the matching workspace runner build.
            let mut authority_connection = sqlx::SqliteConnection::connect(&format!(
                "sqlite://{}",
                directory.path().join("state.sqlite3").display()
            ))
            .await
            .unwrap();
            sqlx::query("UPDATE recovery_authority SET authority_id=?")
                .bind("a".repeat(64))
                .execute(&mut authority_connection)
                .await
                .unwrap();
            let workspace = CheckWorkspace {
                wrapper: std::path::PathBuf::from(
                    std::env::var("CONDUCTOR_TEST_BIN_DIR")
                        .expect("build workspace binaries and set CONDUCTOR_TEST_BIN_DIR"),
                )
                .join("conductor-runner"),
                source,
                inventory,
                destination: directory.path().join("check"),
                toolchain_roots: vec![],
            };
            if !cancel {
                for missing_wrapper in [false, true] {
                    let failed_workspace = CheckWorkspace {
                        wrapper: if missing_wrapper {
                            directory.path().join("absent-runner")
                        } else {
                            workspace.wrapper.clone()
                        },
                        source: workspace.source.clone(),
                        inventory: if missing_wrapper {
                            workspace.inventory.clone()
                        } else {
                            vec!["absent-input".into()]
                        },
                        destination: directory.path().join(if missing_wrapper {
                            "bad-wrapper"
                        } else {
                            "bad-capture"
                        }),
                        toolchain_roots: vec![],
                    };
                    let (_sender, receiver) = tokio::sync::watch::channel(false);
                    assert!(
                        service
                            .execute_required_check(&token, "test", &failed_workspace, receiver)
                            .await
                            .is_err()
                    );
                    assert!(!store.has_pending_auxiliary("task").await.unwrap());
                    let bound: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM auxiliary_invocations WHERE launch_binding_json IS NOT NULL OR process_json IS NOT NULL OR release_recorded!=0").fetch_one(store.readers()).await.unwrap();
                    assert_eq!(bound, 0);
                    let row = store.task("task").await.unwrap().unwrap();
                    let task: Task = serde_json::from_str(&row.aggregate_json).unwrap();
                    assert!(task.verification.unwrap().checks.is_empty());
                }
                let row = store.task("task").await.unwrap().unwrap();
                let task: Task = serde_json::from_str(&row.aggregate_json).unwrap();
                let artifacts = directory.path().join("bad-review");
                fs::create_dir_all(artifacts.join("review-input")).unwrap();
                let source_identity = format!("snapshot:{digest}");
                let invocation = service
                    .begin_review_auxiliary(&task, &input.packet, &source_identity, &artifacts)
                    .await
                    .unwrap();
                let baseline = snapshot_paths(
                    &workspace.source,
                    &directory.path().join("review-baseline-fixture"),
                    &workspace.inventory,
                )
                .unwrap();
                let prepared = crate::workflow_review::prepare_review(
                    &artifacts,
                    &baseline,
                    &workspace.inventory,
                    &source_identity,
                );
                assert!(
                    service
                        .auxiliary_prelaunch_result(&invocation, prepared)
                        .await
                        .is_err()
                );
                assert!(!store.has_pending_auxiliary("task").await.unwrap());
                let abandoned: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM audit_events WHERE kind='auxiliary.not_launched'",
                )
                .fetch_one(store.readers())
                .await
                .unwrap();
                assert_eq!(abandoned, 3);
            }
            let (_sender, receiver) = tokio::sync::watch::channel(false);
            let execute = service.execute_required_check(&token, "test", &workspace, receiver);
            let interrupt = async {
                if cancel {
                    tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        while !workspace.destination.join("tree/marker").exists() {
                            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        }
                    })
                    .await
                    .expect("check started");
                    let _guard = service.mutations.lock().await;
                    let row = store.task("task").await.unwrap().unwrap();
                    let task = serde_json::from_str(&row.aggregate_json).unwrap();
                    service
                        .apply_row(
                            row,
                            lifecycle_command(&task, operator, CommandKind::Cancel, "cancel-check"),
                        )
                        .await
                        .unwrap();
                }
            };
            let (result, ()) = tokio::join!(execute, interrupt);
            let row = store.task("task").await.unwrap().unwrap();
            let task: Task = serde_json::from_str(&row.aggregate_json).unwrap();
            let publications:Vec<(String,String,String)>=sqlx::query_as("SELECT p.invocation_id,a.correlation_id,a.kind FROM auxiliary_publications p JOIN audit_events a ON a.id=p.audit_id").fetch_all(store.readers()).await.unwrap();
            if cancel {
                assert!(publications.is_empty());
            } else {
                assert_eq!(publications.len(), 1);
                assert_eq!(publications[0].0, publications[0].1);
                assert_eq!(publications[0].2, "record_check");
            }
            if cancel {
                assert!(result.is_err());
                assert_eq!(task.state, TaskState::Cancelled);
                assert!(task.verification.unwrap().checks.is_empty());
            } else {
                assert_eq!(result.unwrap().outcome, CheckOutcome::Passed);
                assert_eq!(task.state, TaskState::Verifying);
                let verification = task.verification.unwrap();
                assert_eq!(verification.checks.len(), 1);
                assert!(verification.checks[0].passed);
                assert_eq!(
                    verification.checks[0].recorded_by.0,
                    "conductor-check-executor"
                );
                assert!(verification.verdict.is_none());
                assert_auxiliary_preack_recovery(
                    &service,
                    &store,
                    directory.path(),
                    &workspace.wrapper,
                )
                .await;
            }
        }
    }

    #[tokio::test]
    async fn legacy_host_recovery_reads_native_identity_and_keeps_same_boot_blocked() {
        use sqlx::{Connection, Executor};
        let (directory, store) = lifecycle_store().await;
        let mut c = sqlx::SqliteConnection::connect(&format!(
            "sqlite://{}",
            directory.path().join("state.sqlite3").display()
        ))
        .await
        .unwrap();
        c.execute("INSERT INTO legacy_auxiliary_ownership VALUES('task','verification_without_invocation',0)").await.unwrap();
        let before = store.task("task").await.unwrap().unwrap().aggregate_json;
        for generation in [1, 2] {
            let service = PersistentControlService::start_with_reservation(
                store.clone(),
                100,
                FixedClock::new(0),
                Some((&"a".repeat(64), generation)),
            )
            .await
            .unwrap();
            assert_eq!(service.recover_interrupted_workflows().await.unwrap(), 0);
            assert!(store.has_pending_auxiliary("task").await.unwrap());
            let row:(String,String,String,i64)=sqlx::query_as("SELECT platform,host_id,boot_id,observed_generation FROM legacy_auxiliary_host_witness").fetch_one(&mut c).await.unwrap();
            let native = conductor_runtime::HostEpoch::current().await.unwrap();
            assert_eq!(
                (row.0, row.1, row.2),
                (native.platform, native.host_id, native.boot_id)
            );
            assert_eq!(row.3, 1);
            assert_eq!(
                store.task("task").await.unwrap().unwrap().aggregate_json,
                before
            );
        }
        let violations = store.check_runtime_invariants().await.unwrap();
        assert!(
            violations
                .iter()
                .any(|v| v.code == "legacy_auxiliary_ownership_unknown"
                    && v.context_json.contains("later_native_boot_required"))
        );
    }

    async fn assert_auxiliary_preack_recovery(
        service: &PersistentControlService<FixedClock>,
        store: &Store,
        root: &std::path::Path,
        wrapper: &std::path::Path,
    ) {
        use agent_conductor_store::{AuxiliaryKind, AuxiliaryLaunchBinding, EffectAudit};
        use conductor_runtime::{AuxiliaryLaunch, AuxiliarySession};
        use sha2::{Digest, Sha256};
        use std::os::unix::fs::PermissionsExt;
        let row = store.task("task").await.unwrap().unwrap();
        let task: Task = serde_json::from_str(&row.aggregate_json).unwrap();
        let source = task
            .verification
            .as_ref()
            .unwrap()
            .source
            .as_ref()
            .unwrap()
            .0
            .clone();
        let work = root.join("preack-work");
        fs::create_dir(&work).unwrap();
        let work = work.canonicalize().unwrap();
        let receipts = root.join("preack-receipts");
        fs::create_dir(&receipts).unwrap();
        fs::set_permissions(&receipts, fs::Permissions::from_mode(0o700)).unwrap();
        let input = service
            .begin_auxiliary(&task, AuxiliaryKind::Check, &source, &work, b"preack-test")
            .await
            .unwrap();
        assert_native_auxiliary_origin(store, &input).await;
        let executable = std::path::PathBuf::from("/bin/sh").canonicalize().unwrap();
        let hash = |p: &std::path::Path| format!("{:x}", Sha256::digest(fs::read(p).unwrap()));
        let launch = AuxiliaryLaunch {
            version: 1,
            invocation_id: input.invocation_id.clone(),
            authority_id: "a".repeat(64),
            boot_generation: service.boot_generation,
            receipt_root: receipts.canonicalize().unwrap(),
            executable_sha256: hash(&executable),
            executable,
            arguments: vec!["-c".into(), "echo forbidden > payload-started".into()],
            directory: work.clone(),
            environment: std::collections::BTreeMap::new(),
            timeout_ms: 5000,
            heartbeat_timeout_ms: 3000,
            output_limit: 1024,
        };
        let wrapper = wrapper.canonicalize().unwrap();
        let binding = AuxiliaryLaunchBinding {
            launch_sha256: launch.sha256().unwrap(),
            authority_id: launch.authority_id.clone(),
            receipt_root: launch.receipt_root.to_str().unwrap().into(),
            wrapper_path: wrapper.to_str().unwrap().into(),
            wrapper_sha256: hash(&wrapper),
        };
        store
            .bind_auxiliary_launch(
                &input,
                &binding,
                EffectAudit {
                    id: "preack-bind",
                    actor_id: "test",
                    correlation_id: &input.invocation_id,
                    now_ms: 100,
                },
            )
            .await
            .unwrap();
        let session = AuxiliarySession::prepare(&wrapper, &launch).await.unwrap();
        assert_eq!(service.recover_auxiliary_receipts().await.unwrap(), 0);
        assert!(store.has_pending_auxiliary("task").await.unwrap());
        // Lose the private parent pipe before the Ready identity is acknowledged.
        drop(session);
        let receipt = receipts.join(format!("auxiliary-{}.json", input.invocation_id));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !receipt.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_paged_auxiliary_recovery(service, store, root, &input, &receipt, &work).await;
    }

    async fn assert_native_auxiliary_origin(
        store: &Store,
        input: &agent_conductor_store::AuxiliaryPreparation,
    ) {
        let row:(String,String,Option<String>)=sqlx::query_as("SELECT kind,host_json,ownership_json FROM auxiliary_host_evidence WHERE invocation_id=?").bind(&input.invocation_id).fetch_one(store.readers()).await.unwrap();
        assert_eq!(row.0, "admission");
        assert!(row.2.is_none());
        let host: agent_conductor_store::AuxiliaryHostObservation =
            serde_json::from_str(&row.1).unwrap();
        let native = conductor_runtime::HostEpoch::current().await.unwrap();
        assert_eq!(host.generation, input.boot_generation);
        assert_eq!(host.authority_id, "a".repeat(64));
        assert_eq!(
            (host.platform, host.host_id, host.boot_id),
            (native.platform, native.host_id, native.boot_id)
        );
        let state:(Option<String>,Option<String>,i64)=sqlx::query_as("SELECT launch_binding_json,process_json,release_recorded FROM auxiliary_invocations WHERE invocation_id=?").bind(&input.invocation_id).fetch_one(store.readers()).await.unwrap();
        assert_eq!(
            state,
            (None, None, 0),
            "native host must be durable before launch binding or Release"
        );
    }

    async fn assert_paged_auxiliary_recovery(
        service: &PersistentControlService<FixedClock>,
        store: &Store,
        root: &std::path::Path,
        input: &agent_conductor_store::AuxiliaryPreparation,
        receipt: &std::path::Path,
        work: &std::path::Path,
    ) {
        use std::os::unix::fs::PermissionsExt;
        seed_invalid_auxiliary_page(store, root, &input.execution_token).await;
        let restarted = PersistentControlService::start_with_reservation(
            store.clone(),
            200,
            FixedClock::new(0),
            Some((&"a".repeat(64), service.boot_generation + 1)),
        )
        .await
        .unwrap();
        fs::set_permissions(receipt, fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(restarted.recover_auxiliary_receipts().await.unwrap(), 0);
        assert!(restarted.auxiliary_recovery_cursor.lock().await.is_some());
        // A clone continues the same sweep and refuses the non-private proof.
        assert_eq!(
            restarted
                .clone()
                .recover_auxiliary_receipts()
                .await
                .unwrap(),
            0
        );
        assert!(restarted.auxiliary_recovery_cursor.lock().await.is_none());
        assert!(store.has_pending_auxiliary("task").await.unwrap());
        fs::set_permissions(receipt, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(service.recover_auxiliary_receipts().await.unwrap(), 0);
        assert_eq!(service.recover_auxiliary_receipts().await.unwrap(), 0);
        assert!(store.has_pending_auxiliary("task").await.unwrap());
        assert_eq!(restarted.recover_auxiliary_receipts().await.unwrap(), 0);
        assert_eq!(restarted.recover_auxiliary_receipts().await.unwrap(), 1);
        assert_eq!(restarted.recover_auxiliary_receipts().await.unwrap(), 0);
        assert!(!store.has_pending_auxiliary("task").await.unwrap());
        let observed:(Option<String>,i64,String)=sqlx::query_as("SELECT process_json,release_recorded,settlement_json FROM auxiliary_invocations WHERE invocation_id=?").bind(&input.invocation_id).fetch_one(store.readers()).await.unwrap();
        assert!(observed.0.is_none());
        assert_eq!(observed.1, 0);
        let proof: serde_json::Value = serde_json::from_str(&observed.2).unwrap();
        assert_eq!(proof["observation"], "never_released");
        assert!(!work.join("payload-started").exists());
        let unknown: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM auxiliary_invocations WHERE state='pending'")
                .fetch_one(store.readers())
                .await
                .unwrap();
        assert_eq!(
            unknown, 128,
            "invalid rows must remain owned, never silently settled"
        );
    }

    async fn seed_invalid_auxiliary_page(store: &Store, root: &std::path::Path, token: &str) {
        use sqlx::Connection;
        let mut c = sqlx::SqliteConnection::connect(&format!(
            "sqlite://{}",
            root.join("state.sqlite3").display()
        ))
        .await
        .unwrap();
        for i in 1..=128 {
            let id = format!("invalid-aux-{i:03}");
            let mut task = Task::new(TaskId(id.clone()), vec![]);
            task.state = TaskState::Paused;
            store
                .create_task(NewTask {
                    id: &id,
                    project_id: "project",
                    objective: "invalid ownership fixture",
                    state: "paused",
                    priority: 0,
                    authority_json: "{}",
                    aggregate_json: &serde_json::to_string(&task).unwrap(),
                    created_at_ms: 0,
                })
                .await
                .unwrap();
            let invocation = format!("00000000-0000-4000-8000-{i:012x}");
            sqlx::query("INSERT INTO auxiliary_invocations(invocation_id,task_id,execution_token,preparation_json,boot_generation,state,created_at_ms) VALUES(?,?,?,'{}',1,'pending',0)").bind(invocation).bind(&id).bind(token).execute(&mut c).await.unwrap();
        }
    }

    fn assert_unstarted_binding(
        record: &agent_conductor_store::ExecutionRecord,
        preparation: &agent_conductor_store::ExecutionPreparation,
    ) {
        assert!(record.wrapper_identity.is_none());
        assert!(record.process.is_none());
        assert_eq!(&record.preparation, preparation);
    }

    async fn refuse_unstarted_fence_audit(
        service: &PersistentControlService<FixedClock>,
        store: &Store,
        db: &mut sqlx::SqliteConnection,
        token: &str,
    ) {
        sqlx::query("CREATE TRIGGER reject_unstarted_fence BEFORE INSERT ON audit_events WHEN NEW.kind='execution.fenced' BEGIN SELECT RAISE(ABORT, 'injected fence audit refusal'); END")
            .execute(&mut *db).await.unwrap();
        assert!(service.recover_runtime_observations().await.is_err());
        let blocked = store.execution(token).await.unwrap().unwrap();
        assert_eq!(
            blocked.state,
            agent_conductor_store::ExecutionState::Blocked
        );
        assert!(blocked.wrapper_identity.is_none());
        assert!(store.has_unfenced_execution("task").await.unwrap());
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE kind='execution.fenced'")
                .fetch_one(store.readers())
                .await
                .unwrap();
        assert_eq!(count, 0);
        sqlx::query("DROP TRIGGER reject_unstarted_fence")
            .execute(db)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn unstarted_recovery_requires_durable_identity_and_preserves_unknown_ack() {
        use agent_conductor_store::{EffectAudit, ExecutionPreparation, ExecutionState};
        use conductor_runtime::{AdapterKind, RunnerProcessIdentity, write_unstarted_receipt};
        use sqlx::Connection;
        for mode in [
            "valid",
            "legacy",
            "wrong_wrapper",
            "wrong_authority",
            "commands",
        ] {
            let (directory, store) = lifecycle_store().await;
            let service =
                PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
                    .await
                    .unwrap();
            let (_, token) = claim_task(&service).await;
            let audit = |id| EffectAudit {
                id,
                actor_id: "runtime",
                correlation_id: "fixture",
                now_ms: 100,
            };
            store
                .claim_effect(&token, "runner", 10000, audit("claim"))
                .await
                .unwrap();
            let tree = directory.path().join("tree");
            std::fs::create_dir(&tree).unwrap();
            let mut launch = runtime_launch(&tree.canonicalize().unwrap());
            launch.packet.contract.adapter.kind = AdapterKind::Claude;
            launch.packet.contract.authority.allow_workspace_commands = mode == "commands";
            let expected = Uuid::now_v7().to_string();
            let preparation = ExecutionPreparation {
                expected_wrapper_identity: if mode == "legacy" {
                    None
                } else {
                    Some(expected.clone())
                },
                effect_token: token.clone(),
                task_id: "task".into(),
                attempt_id: "attempt".into(),
                lease_epoch: 1,
                runner_id: "runner".into(),
                packet_json: serde_json::to_string(&launch.packet).unwrap(),
                packet_sha256: launch.packet.sha256().unwrap(),
            };
            store
                .prepare_execution(&preparation, audit("prepare"))
                .await
                .unwrap();
            let mut db = sqlx::SqliteConnection::connect(&format!(
                "sqlite://{}",
                directory.path().join("state.sqlite3").display()
            ))
            .await
            .unwrap();
            sqlx::query("UPDATE recovery_authority SET authority_id=?")
                .bind("a".repeat(64))
                .execute(&mut db)
                .await
                .unwrap();
            let identity = RunnerProcessIdentity {
                authority_id: if mode == "wrong_authority" {
                    "b".repeat(64)
                } else {
                    "a".repeat(64)
                },
                packet_sha256: preparation.packet_sha256.clone(),
                wrapper_identity: if mode == "wrong_wrapper" {
                    Uuid::now_v7().to_string()
                } else {
                    expected
                },
            };
            write_unstarted_receipt(&launch.packet, &identity).unwrap();
            if mode == "valid" {
                refuse_unstarted_fence_audit(&service, &store, &mut db, &token).await;
            }
            service.recover_runtime_observations().await.unwrap();
            let record = store.execution(&token).await.unwrap().unwrap();
            assert_eq!(
                record.state,
                if mode == "valid" {
                    ExecutionState::Fenced
                } else {
                    ExecutionState::Blocked
                }
            );
            assert_unstarted_binding(&record, &preparation);
            assert_eq!(
                store.has_unfenced_execution("task").await.unwrap(),
                mode != "valid"
            );
            service.recover_runtime_observations().await.unwrap();
            assert_eq!(store.execution(&token).await.unwrap().unwrap(), record);
        }
    }

    #[tokio::test]
    async fn restart_blocks_unobserved_runtime_intent_without_spawning_or_losing_packet() {
        use agent_conductor_store::{EffectAudit, ExecutionPreparation, ExecutionState};
        let (directory, store) = lifecycle_store().await;
        let service =
            PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
                .await
                .expect("service");
        let (_, token) = claim_task(&service).await;
        store
            .claim_effect(
                &token,
                "runner",
                10000,
                EffectAudit {
                    id: "claim-runtime",
                    actor_id: "runtime",
                    correlation_id: &token,
                    now_ms: 100,
                },
            )
            .await
            .expect("effect claim");
        let input = runtime_launch(directory.path());
        let preparation = ExecutionPreparation {
            expected_wrapper_identity: None,
            effect_token: token.clone(),
            task_id: "task".into(),
            attempt_id: "attempt".into(),
            lease_epoch: 1,
            runner_id: "runner".into(),
            packet_json: serde_json::to_string(&input.packet).expect("packet"),
            packet_sha256: input.packet.sha256().expect("hash"),
        };
        store
            .prepare_execution(
                &preparation,
                EffectAudit {
                    id: "prepare-runtime",
                    actor_id: "runtime",
                    correlation_id: &token,
                    now_ms: 100,
                },
            )
            .await
            .expect("intent");
        drop(service);
        let service =
            PersistentControlService::start_with_clock(store.clone(), 101, FixedClock::new(0))
                .await
                .expect("restart");
        let record = store
            .execution(&token)
            .await
            .expect("read")
            .expect("record");
        assert_eq!(record.state, ExecutionState::Blocked);
        assert_eq!(record.preparation, preparation);
        let row = store.task("task").await.expect("task").expect("row");
        let task: Task = serde_json::from_str(&row.aggregate_json).expect("aggregate");
        assert_eq!(task.state, TaskState::Blocked);
        assert!(store.has_unfenced_execution("task").await.expect("fence"));
        drop(service);
        PersistentControlService::start_with_clock(store.clone(), 102, FixedClock::new(0))
            .await
            .expect("second restart");
        assert_eq!(
            store
                .execution(&token)
                .await
                .expect("read")
                .expect("record"),
            record
        );
    }
}
