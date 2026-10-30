//! SQLite persistence for Agent Conductor.
//!
//! This crate stores outcomes produced by the domain layer. It deliberately does
//! not decide whether a lifecycle transition is valid.

use std::{path::Path, str::FromStr, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use sqlx::{
    Connection, SqliteConnection, SqlitePool,
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use thiserror::Error;
use tokio::sync::Mutex;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

mod auxiliary;
mod auxiliary_host;
mod legacy_auxiliary_host;
pub use legacy_auxiliary_host::AuxiliaryHostObservation;
mod auxiliary_launch;
mod auxiliary_publication;
pub use auxiliary::{AuxiliaryKind, AuxiliaryPreparation};
pub use auxiliary_launch::{
    AuxiliaryInvocation, AuxiliaryLaunchBinding, AuxiliaryRecoveryCandidate,
};
mod conversation_continuations;
mod conversation_delegations;
pub use conversation_delegations::{ConversationAssignment, ConversationReplyExamination};
mod conversation_jobs;
mod conversation_rooms;
mod conversation_routes;
pub use conversation_routes::{ConversationDelivery, ConversationRecipient, ConversationRouting};
mod conversation_turns;
mod conversations;
pub use conversation_turns::{ConversationReplyOrigin, ConversationTurnIntent};
mod inventory;
pub use conversations::{ConversationHeader, ConversationMessage};
mod usage;
pub use inventory::{InventoryKind, InventoryRow};
mod evidence;
mod history;
pub use evidence::EvidenceRow;
#[cfg(test)]
mod auxiliary_crash_tests;
#[cfg(test)]
mod auxiliary_launch_crash_tests;
#[cfg(test)]
mod broker_launch_crash_tests;
#[cfg(test)]
mod transaction_cancellation;
pub use history::AuditMetadata;
mod workflow_dependencies;
pub use workflow_dependencies::WorkflowDependency;
mod completed_dispatch;
mod workflow_assignments;
mod workflows;
pub use workflows::WorkflowDispatch;

mod account_execution_result;
pub use account_execution_result::AccountExecutionResult;
mod account_execution_peer;
mod executions;
pub use account_execution_peer::AccountExecutionPeer;
pub use executions::{
    ExecutionPreparation, ExecutionProcess, ExecutionRecord, ExecutionState, ExecutionUpdate,
};

mod effects;
pub use effects::{EffectAudit, EffectRecord, EffectStatus};
mod events;
pub use events::{
    ConsumerStateEffect, EVENT_SCHEMA_VERSION, EventDelivery, EventLimits, EventSourceIdentity,
    EventStoreError, EventSubscription, PublishResult, RepositoryObservation, SemanticEvent,
};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("task version conflict for {task_id}; expected {expected_version}")]
    VersionConflict {
        task_id: String,
        expected_version: i64,
    },
    #[error("idempotency key was reused with a different request")]
    IdempotencyCollision,
    #[error("project path, filesystem, or Git identity is already registered")]
    ProjectIdentityConflict,
    #[error(
        "invalid task version step for {task_id}; expected the successor of {expected_version}, got {new_version}"
    )]
    InvalidVersionStep {
        task_id: String,
        expected_version: i64,
        new_version: i64,
    },
    #[error("invalid database path")]
    InvalidPath,
    #[error("database integrity check failed: {0}")]
    Integrity(String),
    #[error("boot generation overflow")]
    BootGenerationOverflow,
    #[error("effect intent not found: {0}")]
    EffectNotFound(String),
    #[error("attempt identity conflict for {0}")]
    AttemptIdentityConflict(String),
    #[error("illegal effect transition for {token}: {detail}")]
    EffectTransition { token: String, detail: String },
}

#[derive(Clone)]
pub struct Store {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    writer: Mutex<SqliteConnection>,
    readers: SqlitePool,
    dispatch_changes: tokio::sync::watch::Sender<()>,
    completed_dispatch_cursor: Mutex<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskMutation {
    pub task_id: String,
    pub expected_version: i64,
    pub new_version: i64,
    pub state: String,
    pub aggregate_json: String,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptLeaseMutation {
    pub attempt_id: String,
    pub task_id: String,
    pub worker_id: String,
    pub lease_epoch: i64,
    pub lease_status: String,
    pub boot_generation: i64,
    pub remaining_ms: i64,
    pub grace_ms: i64,
    pub display_expires_at_ms: i64,
    pub last_heartbeat_at_ms: i64,
    pub instruction_hashes_json: String,
    pub required_checks_json: String,
    pub capability_hash: String,
    pub ended_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRegistration {
    pub id: String,
    pub adapter_kind: String,
    pub protocol_fingerprint: String,
    pub capability_hash: String,
    pub access_mode: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedAgent {
    pub id: String,
    pub adapter_kind: String,
    pub protocol_fingerprint: String,
    pub capability_hash: String,
    pub access_mode: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRegistration {
    pub id: String,
    pub canonical_path: String,
    pub filesystem_identity: String,
    pub git_common_dir_identity: Option<String>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRegistration {
    pub id: String,
    pub project_id: String,
    pub objective: String,
    pub state: String,
    pub priority: i64,
    pub authority_json: String,
    pub aggregate_json: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditAppend {
    pub id: String,
    pub task_id: Option<String>,
    pub actor_id: String,
    pub kind: String,
    pub correlation_id: String,
    pub payload_json: String,
    pub occurred_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTransitionAppend {
    pub id: String,
    pub task_id: String,
    pub actor_id: String,
    pub prior_state: String,
    pub new_state: String,
    pub reason: String,
    pub correlation_id: String,
    pub occurred_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectIntentAppend {
    pub token: String,
    pub task_id: Option<String>,
    pub attempt_id: Option<String>,
    pub lease_epoch: Option<i64>,
    pub correlation_id: String,
    pub effect_kind: String,
    pub request_json: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitBatch {
    pub actor_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub task: Option<TaskMutation>,
    pub project: Option<ProjectRegistration>,
    pub agent: Option<AgentRegistration>,
    pub new_task: Option<TaskRegistration>,
    pub attempt: Option<AttemptLeaseMutation>,
    pub transitions: Vec<TaskTransitionAppend>,
    pub audit: Vec<AuditAppend>,
    pub effects: Vec<EffectIntentAppend>,
    pub outcome_json: String,
    pub committed_at_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitResult {
    Applied { outcome_json: String },
    Replayed { outcome_json: String },
}

#[derive(Debug, Clone)]
pub struct NewProject<'a> {
    pub id: &'a str,
    pub canonical_path: &'a str,
    pub filesystem_identity: &'a str,
    pub git_common_dir_identity: Option<&'a str>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone)]
pub struct NewAgent<'a> {
    pub id: &'a str,
    pub adapter_kind: &'a str,
    pub protocol_fingerprint: &'a str,
    pub capability_hash: &'a str,
    pub access_mode: &'a str,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone)]
pub struct NewTask<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
    pub objective: &'a str,
    pub state: &'a str,
    pub priority: i64,
    pub authority_json: &'a str,
    pub aggregate_json: &'a str,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedTask {
    pub id: String,
    pub project_id: String,
    pub objective: String,
    pub priority: i64,
    pub authority_json: String,
    pub aggregate_json: String,
}

/// Advisory lifecycle discovery data; callers must reload before mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCandidate {
    pub id: String,
    pub aggregate_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStartup {
    pub boot_generation: u64,
    pub started_at_ms: i64,
    pub recovery_pending: Vec<RecoveryLease>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryLease {
    pub task_id: String,
    pub attempt_id: String,
    pub epoch: u64,
    pub recovery_window_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeInvariantViolation {
    pub code: String,
    pub summary: String,
    pub context_json: String,
}

impl Store {
    /// Looks up the registered project by observed filesystem or Git identity.
    /// # Errors
    /// Returns database errors without changing project registration.
    pub async fn project_for_identity(
        &self,
        filesystem: &str,
        git_common: Option<&str>,
    ) -> Result<Option<String>, StoreError> {
        Ok(sqlx::query_scalar("SELECT id FROM projects WHERE filesystem_identity = ? OR git_common_dir_identity = ? LIMIT 1")
            .bind(filesystem).bind(git_common).fetch_optional(&self.inner.readers).await?)
    }

    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let path_text = path.to_str().ok_or(StoreError::InvalidPath)?;
        let url = format!("sqlite://{path_text}");
        let options = SqliteConnectOptions::from_str(&url)?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5));

        let mut writer = SqliteConnection::connect_with(&options).await?;
        MIGRATOR.run(&mut writer).await?;

        let readers = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options.read_only(true))
            .await?;

        Ok(Self {
            inner: Arc::new(StoreInner {
                writer: Mutex::new(writer),
                readers,
                dispatch_changes: tokio::sync::watch::channel(()).0,
                completed_dispatch_cursor: Mutex::new(None),
            }),
        })
    }

    pub fn readers(&self) -> &SqlitePool {
        &self.inner.readers
    }

    /// Reads the current binding and generation for external boot reservation.
    /// # Errors
    /// Refuses a missing singleton or an invalid persisted generation.
    pub async fn runtime_authority(&self) -> Result<(Option<String>, u64), StoreError> {
        let (binding, generation): (Option<String>, i64) = sqlx::query_as(
            "SELECT a.authority_id,r.boot_generation FROM recovery_authority a JOIN runtime_metadata r ON a.singleton=r.singleton WHERE a.singleton=1",
        ).fetch_one(&self.inner.readers).await?;
        Ok((
            binding,
            u64::try_from(generation)
                .map_err(|_| StoreError::Integrity("negative boot generation".into()))?,
        ))
    }

    pub async fn create_project(&self, project: NewProject<'_>) -> Result<(), StoreError> {
        let mut connection = self.inner.writer.lock().await;
        let mut writer = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = sqlx::query(
            "INSERT INTO projects (id, canonical_path, filesystem_identity, git_common_dir_identity, created_at_ms) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(project.id)
        .bind(project.canonical_path)
        .bind(project.filesystem_identity)
        .bind(project.git_common_dir_identity)
        .bind(project.created_at_ms)
        .execute(&mut *writer)
        .await
        .map(|_| ())
        .map_err(project_insert_error);
        finish_transaction(writer, result).await
    }

    pub async fn create_agent(&self, agent: NewAgent<'_>) -> Result<(), StoreError> {
        let mut writer = self.inner.writer.lock().await;
        sqlx::query("INSERT INTO agents (id, adapter_kind, protocol_fingerprint, capability_hash, access_mode, created_at_ms) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(agent.id)
            .bind(agent.adapter_kind)
            .bind(agent.protocol_fingerprint)
            .bind(agent.capability_hash)
            .bind(agent.access_mode)
            .bind(agent.created_at_ms)
            .execute(&mut *writer)
            .await?;
        Ok(())
    }

    pub async fn agent_exists(&self, agent_id: &str) -> Result<bool, StoreError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agents WHERE id = ?")
            .bind(agent_id)
            .fetch_one(&self.inner.readers)
            .await?;
        Ok(count == 1)
    }

    /// Loads the immutable identity material frozen into a task attempt.
    ///
    /// # Errors
    /// Returns a database error when the agent record cannot be read.
    pub async fn agent(&self, agent_id: &str) -> Result<Option<PersistedAgent>, StoreError> {
        sqlx::query_as::<_, (String, String, String, String, String)>(
            "SELECT id, capability_hash, access_mode, adapter_kind, protocol_fingerprint FROM agents WHERE id = ?",
        )
        .bind(agent_id)
        .fetch_optional(&self.inner.readers)
        .await
        .map(|row| {
            row.map(|(id, capability_hash, access_mode, adapter_kind, protocol_fingerprint)| PersistedAgent {
                id,
                adapter_kind,
                protocol_fingerprint,
                capability_hash,
                access_mode,
            })
        })
        .map_err(StoreError::from)
    }

    pub async fn create_task(&self, task: NewTask<'_>) -> Result<(), StoreError> {
        let mut connection = self.inner.writer.lock().await;
        let mut writer = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = sqlx::query(
            "INSERT INTO tasks (id, project_id, objective, state, priority, version, authority_json, aggregate_json, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?)",
        )
        .bind(task.id)
        .bind(task.project_id)
        .bind(task.objective)
        .bind(task.state)
        .bind(task.priority)
        .bind(task.authority_json)
        .bind(task.aggregate_json)
        .bind(task.created_at_ms)
        .bind(task.created_at_ms)
        .execute(&mut *writer)
        .await
        .map(|_| ())
        .map_err(StoreError::from);
        finish_transaction(writer, result).await
    }

    pub async fn task(&self, task_id: &str) -> Result<Option<PersistedTask>, StoreError> {
        sqlx::query_as::<_, (String, String, String, i64, String, String)>(
            "SELECT id, project_id, objective, priority, authority_json, aggregate_json FROM tasks WHERE id = ? AND aggregate_json IS NOT NULL",
        )
        .bind(task_id)
        .fetch_optional(&self.inner.readers)
        .await
        .map(|row| row.map(task_from_row))
        .map_err(StoreError::from)
    }

    pub async fn tasks(&self, project_id: Option<&str>) -> Result<Vec<PersistedTask>, StoreError> {
        let rows = if let Some(project_id) = project_id {
            sqlx::query_as::<_, (String, String, String, i64, String, String)>(
                "SELECT id, project_id, objective, priority, authority_json, aggregate_json FROM tasks WHERE project_id = ? AND aggregate_json IS NOT NULL ORDER BY priority DESC, id",
            )
            .bind(project_id)
            .fetch_all(&self.inner.readers)
            .await?
        } else {
            sqlx::query_as::<_, (String, String, String, i64, String, String)>(
                "SELECT id, project_id, objective, priority, authority_json, aggregate_json FROM tasks WHERE aggregate_json IS NOT NULL ORDER BY priority DESC, id",
            )
            .fetch_all(&self.inner.readers)
            .await?
        };
        Ok(rows.into_iter().map(task_from_row).collect())
    }

    /// Reads all aggregate-bearing tasks in the same order as `tasks(None)`,
    /// without copying objective, authority or project metadata for discovery.
    /// # Errors
    /// Returns database errors. Aggregate validation remains the caller's job.
    pub async fn task_candidates(&self) -> Result<Vec<TaskCandidate>, StoreError> {
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT id, aggregate_json FROM tasks WHERE aggregate_json IS NOT NULL ORDER BY priority DESC, id",
        )
        .fetch_all(&self.inner.readers)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(id, aggregate_json)| TaskCandidate { id, aggregate_json })
            .collect())
    }

    /// Reads a bounded task page in immutable task-ID order, plus one sentinel.
    /// # Errors
    /// Returns database errors.
    pub async fn task_page(
        &self,
        project_id: Option<&str>,
        after: Option<&str>,
    ) -> Result<Vec<PersistedTask>, StoreError> {
        let rows = if let Some(project_id) = project_id {
            sqlx::query_as::<_, (String, String, String, i64, String, String)>(
                "SELECT id, project_id, objective, priority, authority_json, aggregate_json FROM tasks WHERE project_id=? AND id>? AND aggregate_json IS NOT NULL ORDER BY id LIMIT 101")
                .bind(project_id).bind(after.unwrap_or("")).fetch_all(&self.inner.readers).await?
        } else {
            sqlx::query_as::<_, (String, String, String, i64, String, String)>(
                "SELECT id, project_id, objective, priority, authority_json, aggregate_json FROM tasks WHERE id>? AND aggregate_json IS NOT NULL ORDER BY id LIMIT 101")
                .bind(after.unwrap_or("")).fetch_all(&self.inner.readers).await?
        };
        Ok(rows.into_iter().map(task_from_row).collect())
    }

    pub async fn command_outcome(
        &self,
        actor_id: &str,
        idempotency_key: &str,
        request_hash: &str,
    ) -> Result<Option<String>, StoreError> {
        let existing = sqlx::query_as::<_, (String, String)>(
            "SELECT request_hash, outcome_json FROM command_idempotency WHERE actor_id = ? AND idempotency_key = ?",
        )
        .bind(actor_id)
        .bind(idempotency_key)
        .fetch_optional(&self.inner.readers)
        .await?;
        match existing {
            Some((stored_hash, _)) if stored_hash != request_hash => {
                Err(StoreError::IdempotencyCollision)
            }
            Some((_, outcome)) => Ok(Some(outcome)),
            None => Ok(None),
        }
    }

    pub async fn persist(&self, batch: &CommitBatch) -> Result<CommitResult, StoreError> {
        let mut connection = self.inner.writer.lock().await;
        let mut writer = connection.begin_with("BEGIN IMMEDIATE").await?;

        let result = persist_in_transaction(&mut writer, batch).await;
        finish_transaction(writer, result).await
    }

    /// Persists an ordered set of domain batches atomically. A late conflict or
    /// invalid append rolls back earlier batches, including their replay records.
    /// # Errors
    /// Returns the first persistence error; no batch is committed on failure.
    pub async fn persist_sequence(
        &self,
        batches: &[CommitBatch],
    ) -> Result<Vec<CommitResult>, StoreError> {
        let mut connection = self.inner.writer.lock().await;
        let mut writer = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = async {
            let mut results = Vec::with_capacity(batches.len());
            for batch in batches {
                results.push(persist_in_transaction(&mut writer, batch).await?);
            }
            Ok(results)
        }
        .await;
        finish_transaction(writer, result).await
    }

    /// Verifies the database and atomically begins a new daemon boot era.
    ///
    /// # Errors
    ///
    /// Refuses startup when SQLite reports corruption, dangling foreign keys,
    /// the runtime singleton is missing, or the generation cannot advance.
    pub async fn start_runtime(&self, started_at_ms: i64) -> Result<RuntimeStartup, StoreError> {
        self.start_runtime_generation(started_at_ms, None).await
    }

    /// Begins a boot using an already durable external generation reservation.
    /// The caller must retain exclusive ownership of that external authority.
    /// # Errors
    /// Refuses stale reservations, changed authority bindings and invalid state.
    pub async fn start_reserved_runtime(
        &self,
        started_at_ms: i64,
        authority_id: &str,
        reserved_generation: u64,
    ) -> Result<RuntimeStartup, StoreError> {
        if authority_id.len() != 64 || !authority_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(StoreError::Integrity(
                "invalid recovery authority identity".into(),
            ));
        }
        let generation =
            i64::try_from(reserved_generation).map_err(|_| StoreError::BootGenerationOverflow)?;
        if generation <= 0 {
            return Err(StoreError::BootGenerationOverflow);
        }
        self.start_runtime_generation(started_at_ms, Some((authority_id, generation)))
            .await
    }

    async fn start_runtime_generation(
        &self,
        started_at_ms: i64,
        reservation: Option<(&str, i64)>,
    ) -> Result<RuntimeStartup, StoreError> {
        let mut connection = self.inner.writer.lock().await;
        let mut writer = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = async {
            verify_integrity(&mut writer).await?;
            let binding: Option<String> = sqlx::query_scalar(
                "SELECT authority_id FROM recovery_authority WHERE singleton=1",
            ).fetch_one(&mut *writer).await?;
            if let Some((identity, _)) = reservation {
                if binding.as_deref().is_some_and(|bound| bound != identity) {
                    return Err(StoreError::Integrity("recovery authority identity changed".into()));
                }
                sqlx::query("UPDATE recovery_authority SET authority_id=? WHERE singleton=1")
                    .bind(identity).execute(&mut *writer).await?;
            } else if binding.is_some() {
                return Err(StoreError::Integrity("bound runtime requires an external generation reservation".into()));
            }
            let reserved = reservation.map(|(_, generation)| generation);
            let generation = sqlx::query_scalar::<_, i64>(
                "UPDATE runtime_metadata SET boot_generation = COALESCE(?, boot_generation + 1), last_started_at_ms = ? WHERE singleton = 1 AND boot_generation >= 0 AND boot_generation < COALESCE(?, 9223372036854775807) RETURNING boot_generation",
            )
            .bind(reserved)
            .bind(started_at_ms)
            .bind(reserved)
            .fetch_optional(&mut *writer)
            .await?
            .ok_or(StoreError::BootGenerationOverflow)?;
            let boot_generation = u64::try_from(generation)
                .map_err(|_| StoreError::Integrity("negative boot generation".to_owned()))?;
            sqlx::query(
                "UPDATE leases SET status = 'recovery_pending', boot_generation = ?, remaining_ms = MIN(remaining_ms, grace_ms), display_expires_at_ms = ? + MIN(remaining_ms, grace_ms) WHERE status IN ('active', 'recovery_pending')",
            )
            .bind(generation)
            .bind(started_at_ms)
            .execute(&mut *writer)
            .await?;
            let rows = sqlx::query_as::<_, (String, String, i64, i64)>(
                "SELECT task_id, attempt_id, epoch, remaining_ms FROM leases WHERE status = 'recovery_pending' AND boot_generation = ? ORDER BY task_id",
            )
            .bind(generation)
            .fetch_all(&mut *writer)
            .await?;
            let mut recovery_pending = Vec::with_capacity(rows.len());
            for (task_id, attempt_id, epoch, recovery_window_ms) in rows {
                recovery_pending.push(RecoveryLease {
                    task_id,
                    attempt_id,
                    epoch: u64::try_from(epoch)
                        .map_err(|_| StoreError::Integrity("negative lease epoch".to_owned()))?,
                    recovery_window_ms: u64::try_from(recovery_window_ms).map_err(|_| {
                        StoreError::Integrity("negative recovery window".to_owned())
                    })?,
                });
            }
            Ok(RuntimeStartup {
                boot_generation,
                started_at_ms,
                recovery_pending,
            })
        }
        .await;
        finish_transaction(writer, result).await
    }

    /// Reads persisted state and reports structural contradictions.
    ///
    /// # Errors
    ///
    /// Returns a database error when the invariant snapshot cannot be read.
    pub async fn check_runtime_invariants(
        &self,
    ) -> Result<Vec<RuntimeInvariantViolation>, StoreError> {
        let mut violations = Vec::new();
        let integrity: Vec<String> = sqlx::query_scalar("PRAGMA quick_check")
            .fetch_all(&self.inner.readers)
            .await?;
        for result in integrity.into_iter().filter(|result| result != "ok") {
            violations.push(RuntimeInvariantViolation {
                code: "sqlite_integrity".to_owned(),
                summary: "SQLite quick_check reported corruption".to_owned(),
                context_json: serde_json::json!({"result": result}).to_string(),
            });
        }

        let foreign_key_failures: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pragma_foreign_key_check")
                .fetch_one(&self.inner.readers)
                .await?;
        if foreign_key_failures != 0 {
            violations.push(RuntimeInvariantViolation {
                code: "foreign_key_violation".to_owned(),
                summary: "persisted rows contain dangling foreign keys".to_owned(),
                context_json: serde_json::json!({"count": foreign_key_failures}).to_string(),
            });
        }

        let runtime_generation: Option<i64> = sqlx::query_scalar(
            "SELECT boot_generation FROM runtime_metadata WHERE singleton = 1 AND boot_generation > 0",
        )
        .fetch_optional(&self.inner.readers)
        .await?;
        if runtime_generation.is_none() {
            violations.push(RuntimeInvariantViolation {
                code: "runtime_generation".to_owned(),
                summary: "runtime boot generation is not initialized".to_owned(),
                context_json: serde_json::json!({"valid_rows": 0}).to_string(),
            });
        }

        let task_rows = sqlx::query_as::<_, (String, String, i64, String)>(
            "SELECT id, state, version, aggregate_json FROM tasks WHERE aggregate_json IS NOT NULL ORDER BY id",
        )
        .fetch_all(&self.inner.readers)
        .await?;
        for (task_id, state, version, aggregate_json) in task_rows {
            let aggregate: serde_json::Value =
                serde_json::from_str(&aggregate_json).map_err(|e| {
                    StoreError::Integrity(format!("task {task_id} has invalid aggregate JSON: {e}"))
                })?;
            let matches_projection = aggregate.get("id").and_then(serde_json::Value::as_str)
                == Some(task_id.as_str())
                && aggregate.get("state").and_then(serde_json::Value::as_str)
                    == Some(state.as_str())
                && aggregate.get("version").and_then(serde_json::Value::as_i64) == Some(version);
            if !matches_projection {
                violations.push(RuntimeInvariantViolation {
                    code: "task_projection_mismatch".to_owned(),
                    summary: "task row disagrees with its authoritative aggregate".to_owned(),
                    context_json: serde_json::json!({"task_id": task_id}).to_string(),
                });
            }
            if state == "completed" && !completion_projection_is_valid(&aggregate) {
                violations.push(RuntimeInvariantViolation {
                    code: "incomplete_completed_task".to_owned(),
                    summary:
                        "completed task lacks closed decisions, a closed lease or accepted source-bound verification"
                            .to_owned(),
                    context_json: serde_json::json!({"task_id": task_id}).to_string(),
                });
            }
        }

        let live_rows = sqlx::query_as::<_, (String, String, i64, String, i64, Option<String>, Option<String>, Option<i64>, Option<String>)>(
            "SELECT l.attempt_id, l.task_id, l.epoch, l.status, l.boot_generation, a.task_id, a.worker_id, a.lease_epoch, t.aggregate_json FROM leases l LEFT JOIN attempts a ON a.id = l.attempt_id LEFT JOIN tasks t ON t.id = l.task_id WHERE l.status IN ('active', 'recovery_pending') ORDER BY l.task_id",
        )
        .fetch_all(&self.inner.readers)
        .await?;
        for (
            attempt_id,
            task_id,
            epoch,
            status,
            boot_generation,
            attempt_task_id,
            worker_id,
            attempt_epoch,
            aggregate_json,
        ) in live_rows
        {
            let row_identity_matches = attempt_task_id.as_deref() == Some(task_id.as_str())
                && attempt_epoch == Some(epoch);
            let aggregate_matches = aggregate_json
                .as_deref()
                .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
                .is_some_and(|aggregate| {
                    let attempt = aggregate.get("attempt");
                    attempt
                        .and_then(|v| v.get("id"))
                        .and_then(serde_json::Value::as_str)
                        == Some(attempt_id.as_str())
                        && attempt
                            .and_then(|v| v.get("worker"))
                            .and_then(serde_json::Value::as_str)
                            == worker_id.as_deref()
                        && attempt
                            .and_then(|v| v.get("lease"))
                            .and_then(|v| v.get("epoch"))
                            .and_then(serde_json::Value::as_i64)
                            == Some(epoch)
                        && attempt
                            .and_then(|v| v.get("lease"))
                            .and_then(|v| v.get("boot_generation"))
                            .and_then(serde_json::Value::as_i64)
                            == Some(boot_generation)
                        && attempt
                            .and_then(|v| v.get("lease"))
                            .and_then(|v| v.get("status"))
                            .and_then(serde_json::Value::as_str)
                            == Some(status.as_str())
                });
            if !row_identity_matches || !aggregate_matches {
                violations.push(RuntimeInvariantViolation {
                    code: "lease_identity_mismatch".to_owned(),
                    summary: "live lease, attempt, and task aggregate identities disagree"
                        .to_owned(),
                    context_json: serde_json::json!({"task_id": task_id, "attempt_id": attempt_id})
                        .to_string(),
                });
            }
            if runtime_generation.is_some_and(|generation| generation != boot_generation) {
                violations.push(RuntimeInvariantViolation {
                    code: "lease_boot_generation_mismatch".to_owned(),
                    summary: "live lease belongs to a previous daemon boot".to_owned(),
                    context_json: serde_json::json!({"task_id": task_id, "attempt_id": attempt_id, "boot_generation": boot_generation}).to_string(),
                });
            }
        }

        let tasks_without_live_attempt: Vec<String> = sqlx::query_scalar(
            "SELECT t.id FROM tasks t LEFT JOIN leases l ON l.task_id = t.id AND l.status IN ('active', 'recovery_pending') WHERE t.state IN ('claimed', 'running', 'verifying') GROUP BY t.id HAVING COUNT(l.attempt_id) != 1 ORDER BY t.id",
        )
        .fetch_all(&self.inner.readers)
        .await?;
        for task_id in tasks_without_live_attempt {
            violations.push(RuntimeInvariantViolation {
                code: "task_without_live_attempt".to_owned(),
                summary: "owned task does not have exactly one live lease".to_owned(),
                context_json: serde_json::json!({"task_id": task_id}).to_string(),
            });
        }

        let unsafe_effects: Vec<String> = sqlx::query_scalar(
            "SELECT e.token FROM effect_intents e LEFT JOIN attempts a ON a.id = e.attempt_id WHERE (e.status = 'done' AND e.observation_json IS NULL) OR (e.status IN ('pending', 'claimed', 'observed') AND e.attempt_id IS NOT NULL AND (a.id IS NULL OR a.ended_at_ms IS NOT NULL)) ORDER BY e.token",
        )
        .fetch_all(&self.inner.readers)
        .await?;
        for token in unsafe_effects {
            violations.push(RuntimeInvariantViolation {
                code: "effect_ownership_mismatch".to_owned(),
                summary: "effect status contradicts its observation or owning attempt".to_owned(),
                context_json: serde_json::json!({"token": token}).to_string(),
            });
        }

        let completed_with_live_lease: Vec<String> = sqlx::query_scalar(
            "SELECT tasks.id FROM tasks JOIN leases ON leases.task_id = tasks.id WHERE tasks.state = 'completed' AND leases.status IN ('active', 'recovery_pending') ORDER BY tasks.id",
        )
        .fetch_all(&self.inner.readers)
        .await?;
        for task_id in completed_with_live_lease {
            violations.push(RuntimeInvariantViolation {
                code: "completed_with_live_lease".to_owned(),
                summary: "completed task retains a live lease".to_owned(),
                context_json: serde_json::json!({"task_id": task_id}).to_string(),
            });
        }

        let legacy_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM legacy_auxiliary_ownership")
                .fetch_one(&self.inner.readers)
                .await?;
        if legacy_count != 0 {
            let tasks: Vec<(String,String,String)> = sqlx::query_as("SELECT substr(l.task_id,1,256),l.reason,CASE WHEN w.task_id IS NULL THEN 'native_witness_required' WHEN w.status='observed' THEN 'later_native_boot_required' ELSE 'witness_contradiction' END FROM legacy_auxiliary_ownership l LEFT JOIN legacy_auxiliary_host_witness w ON w.task_id=l.task_id ORDER BY l.task_id LIMIT 128")
                .fetch_all(&self.inner.readers).await?;
            violations.push(RuntimeInvariantViolation {
                code: "legacy_auxiliary_ownership_unknown".into(),
                summary: "legacy verification lacks auxiliary settlement evidence; continuation and backup remain refused".into(),
                context_json: serde_json::json!({"count":legacy_count,"tasks":tasks,"limit":128}).to_string(),
            });
        }

        if let Some(diagnostic) = self.auxiliary_recovery_diagnostic().await? {
            violations.push(diagnostic);
        }
        if let Some(diagnostic) = self.file_broker_recovery_diagnostic().await? {
            violations.push(diagnostic);
        }

        Ok(violations)
    }
}

fn completion_projection_is_valid(aggregate: &serde_json::Value) -> bool {
    let worker = aggregate
        .pointer("/attempt/worker")
        .and_then(serde_json::Value::as_str);
    let source = aggregate
        .pointer("/verification/source")
        .and_then(serde_json::Value::as_str);
    let verdict = aggregate.pointer("/verification/verdict");
    let verdict_valid = verdict
        .and_then(|value| value.get("accepted"))
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        && verdict
            .and_then(|value| value.get("source"))
            .and_then(serde_json::Value::as_str)
            == source
        && verdict
            .and_then(|value| value.get("verifier"))
            .and_then(serde_json::Value::as_str)
            .is_some_and(|verifier| Some(verifier) != worker);
    let lease_ended = aggregate
        .pointer("/attempt/lease/status")
        .and_then(serde_json::Value::as_str)
        == Some("ended");
    let checks = aggregate
        .pointer("/verification/checks")
        .and_then(serde_json::Value::as_array);
    let required = aggregate
        .get("required_checks")
        .and_then(serde_json::Value::as_array);
    let checks_valid = required.zip(checks).is_some_and(|(required, checks)| {
        required.iter().all(|required_check| {
            let id = required_check.get("id").and_then(serde_json::Value::as_str);
            checks.iter().any(|check| {
                check.get("check_id").and_then(serde_json::Value::as_str) == id
                    && check.get("passed").and_then(serde_json::Value::as_bool) == Some(true)
                    && check.get("source").and_then(serde_json::Value::as_str) == source
                    && check
                        .get("recorded_by")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|recorded_by| Some(recorded_by) != worker)
            })
        })
    });
    let decisions_closed = aggregate.get("decisions").is_none_or(|value| {
        value.as_array().is_some_and(|decisions| {
            decisions.iter().all(|decision| {
                decision
                    .pointer("/resolution/rationale")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|text| !text.trim().is_empty())
                    && decision
                        .pointer("/resolution/resolved_by")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|actor| !actor.is_empty() && Some(actor) != worker)
            })
        })
    });
    source.is_some() && verdict_valid && lease_ended && checks_valid && decisions_closed
}

async fn verify_integrity(connection: &mut SqliteConnection) -> Result<(), StoreError> {
    let results: Vec<String> = sqlx::query_scalar("PRAGMA quick_check")
        .fetch_all(&mut *connection)
        .await?;
    if results.as_slice() != ["ok"] {
        return Err(StoreError::Integrity(results.join("; ")));
    }
    let foreign_key_failures: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pragma_foreign_key_check")
            .fetch_one(&mut *connection)
            .await?;
    if foreign_key_failures != 0 {
        return Err(StoreError::Integrity(format!(
            "{foreign_key_failures} foreign key violation(s)"
        )));
    }
    Ok(())
}

fn task_from_row(row: (String, String, String, i64, String, String)) -> PersistedTask {
    PersistedTask {
        id: row.0,
        project_id: row.1,
        objective: row.2,
        priority: row.3,
        authority_json: row.4,
        aggregate_json: row.5,
    }
}

async fn finish_transaction<T>(
    transaction: sqlx::Transaction<'_, sqlx::Sqlite>,
    result: Result<T, StoreError>,
) -> Result<T, StoreError> {
    let value = result?;
    transaction.commit().await?;
    Ok(value)
}

fn project_insert_error(error: sqlx::Error) -> StoreError {
    if error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
    {
        StoreError::ProjectIdentityConflict
    } else {
        StoreError::Database(error)
    }
}

async fn persist_in_transaction(
    connection: &mut SqliteConnection,
    batch: &CommitBatch,
) -> Result<CommitResult, StoreError> {
    if let Some(existing) = sqlx::query_as::<_, (String, String)>(
        "SELECT request_hash, outcome_json FROM command_idempotency WHERE actor_id = ? AND idempotency_key = ?",
    )
    .bind(&batch.actor_id)
    .bind(&batch.idempotency_key)
    .fetch_optional(&mut *connection)
    .await?
    {
        if existing.0 != batch.request_hash {
            return Err(StoreError::IdempotencyCollision);
        }
        return Ok(CommitResult::Replayed {
            outcome_json: existing.1,
        });
    }

    if let Some(project) = &batch.project {
        sqlx::query("INSERT INTO projects (id, canonical_path, filesystem_identity, git_common_dir_identity, created_at_ms) VALUES (?, ?, ?, ?, ?)")
            .bind(&project.id)
            .bind(&project.canonical_path)
            .bind(&project.filesystem_identity)
            .bind(&project.git_common_dir_identity)
            .bind(project.created_at_ms)
            .execute(&mut *connection)
            .await
            .map_err(project_insert_error)?;
    }

    if let Some(agent) = &batch.agent {
        sqlx::query("INSERT INTO agents (id, adapter_kind, protocol_fingerprint, capability_hash, access_mode, created_at_ms) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(&agent.id)
            .bind(&agent.adapter_kind)
            .bind(&agent.protocol_fingerprint)
            .bind(&agent.capability_hash)
            .bind(&agent.access_mode)
            .bind(agent.created_at_ms)
            .execute(&mut *connection)
            .await?;
    }

    if let Some(task) = &batch.new_task {
        sqlx::query("INSERT INTO tasks (id, project_id, objective, state, priority, version, authority_json, aggregate_json, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?)")
            .bind(&task.id)
            .bind(&task.project_id)
            .bind(&task.objective)
            .bind(&task.state)
            .bind(task.priority)
            .bind(&task.authority_json)
            .bind(&task.aggregate_json)
            .bind(task.created_at_ms)
            .bind(task.created_at_ms)
            .execute(&mut *connection)
            .await?;
    }

    if let Some(task) = &batch.task {
        if task.expected_version.checked_add(1) != Some(task.new_version) {
            return Err(StoreError::InvalidVersionStep {
                task_id: task.task_id.clone(),
                expected_version: task.expected_version,
                new_version: task.new_version,
            });
        }
        let result = sqlx::query(
            "UPDATE tasks SET state = ?, version = ?, aggregate_json = ?, updated_at_ms = ? WHERE id = ? AND version = ?",
        )
        .bind(&task.state)
        .bind(task.new_version)
        .bind(&task.aggregate_json)
        .bind(task.updated_at_ms)
        .bind(&task.task_id)
        .bind(task.expected_version)
        .execute(&mut *connection)
        .await?;

        if result.rows_affected() != 1 {
            return Err(StoreError::VersionConflict {
                task_id: task.task_id.clone(),
                expected_version: task.expected_version,
            });
        }
    }

    if let Some(attempt) = &batch.attempt {
        let attempt_result = sqlx::query(
            "INSERT INTO attempts (id, task_id, worker_id, lease_epoch, instruction_hashes_json, required_checks_json, capability_hash, started_at_ms, ended_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET ended_at_ms = excluded.ended_at_ms WHERE attempts.task_id = excluded.task_id AND attempts.worker_id = excluded.worker_id AND attempts.lease_epoch = excluded.lease_epoch AND attempts.instruction_hashes_json = excluded.instruction_hashes_json AND attempts.required_checks_json = excluded.required_checks_json AND attempts.capability_hash = excluded.capability_hash",
        )
        .bind(&attempt.attempt_id)
        .bind(&attempt.task_id)
        .bind(&attempt.worker_id)
        .bind(attempt.lease_epoch)
        .bind(&attempt.instruction_hashes_json)
        .bind(&attempt.required_checks_json)
        .bind(&attempt.capability_hash)
        .bind(attempt.last_heartbeat_at_ms)
        .bind(attempt.ended_at_ms)
        .execute(&mut *connection)
        .await?;
        if attempt_result.rows_affected() != 1 {
            return Err(StoreError::AttemptIdentityConflict(
                attempt.attempt_id.clone(),
            ));
        }
        let lease_result = sqlx::query(
            "INSERT INTO leases (attempt_id, task_id, epoch, status, boot_generation, remaining_ms, grace_ms, display_expires_at_ms, last_heartbeat_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(attempt_id) DO UPDATE SET status = excluded.status, boot_generation = excluded.boot_generation, remaining_ms = excluded.remaining_ms, grace_ms = excluded.grace_ms, display_expires_at_ms = excluded.display_expires_at_ms, last_heartbeat_at_ms = excluded.last_heartbeat_at_ms WHERE leases.task_id = excluded.task_id AND leases.epoch = excluded.epoch",
        )
        .bind(&attempt.attempt_id)
        .bind(&attempt.task_id)
        .bind(attempt.lease_epoch)
        .bind(&attempt.lease_status)
        .bind(attempt.boot_generation)
        .bind(attempt.remaining_ms)
        .bind(attempt.grace_ms)
        .bind(attempt.display_expires_at_ms)
        .bind(attempt.last_heartbeat_at_ms)
        .execute(&mut *connection)
        .await?;
        if lease_result.rows_affected() != 1 {
            return Err(StoreError::AttemptIdentityConflict(
                attempt.attempt_id.clone(),
            ));
        }

        if attempt.ended_at_ms.is_some() {
            let obsolete = sqlx::query_as::<_, (String, String)>(
                "SELECT token, correlation_id FROM effect_intents WHERE task_id = ? AND attempt_id = ? AND lease_epoch = ? AND status IN ('pending', 'claimed', 'observed') ORDER BY token",
            )
            .bind(&attempt.task_id)
            .bind(&attempt.attempt_id)
            .bind(attempt.lease_epoch)
            .fetch_all(&mut *connection)
            .await?;
            for (token, correlation_id) in obsolete {
                let reason = serde_json::json!({
                    "reason": "owning attempt ended before effect completion",
                    "attempt_id": attempt.attempt_id,
                    "lease_epoch": attempt.lease_epoch,
                })
                .to_string();
                sqlx::query(
                    "UPDATE effect_intents SET status = 'abandoned', observation_json = ?, updated_at_ms = ? WHERE token = ? AND status IN ('pending', 'claimed', 'observed')",
                )
                .bind(&reason)
                .bind(attempt.ended_at_ms)
                .bind(&token)
                .execute(&mut *connection)
                .await?;
                sqlx::query(
                    "INSERT INTO audit_events (id, task_id, actor_id, kind, correlation_id, payload_json, occurred_at_ms) VALUES (?, ?, ?, 'effect.abandoned', ?, ?, ?)",
                )
                .bind(format!(
                    "effect-invalidated:{}:{}:{}",
                    batch.actor_id, batch.idempotency_key, token
                ))
                .bind(&attempt.task_id)
                .bind(&batch.actor_id)
                .bind(correlation_id)
                .bind(&reason)
                .bind(attempt.ended_at_ms)
                .execute(&mut *connection)
                .await?;
            }
        }
    }

    for event in &batch.audit {
        sqlx::query(
            "INSERT INTO audit_events (id, task_id, actor_id, kind, correlation_id, payload_json, occurred_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&event.id)
        .bind(&event.task_id)
        .bind(&event.actor_id)
        .bind(&event.kind)
        .bind(&event.correlation_id)
        .bind(&event.payload_json)
        .bind(event.occurred_at_ms)
        .execute(&mut *connection)
        .await?;
    }

    for effect in &batch.effects {
        sqlx::query(
            "INSERT INTO effect_intents (token, task_id, attempt_id, lease_epoch, correlation_id, effect_kind, request_json, status, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
        )
        .bind(&effect.token)
        .bind(&effect.task_id)
        .bind(&effect.attempt_id)
        .bind(effect.lease_epoch)
        .bind(&effect.correlation_id)
        .bind(&effect.effect_kind)
        .bind(&effect.request_json)
        .bind(effect.created_at_ms)
        .bind(effect.created_at_ms)
        .execute(&mut *connection)
        .await?;
    }

    for transition in &batch.transitions {
        sqlx::query(
            "INSERT INTO task_transitions (id, task_id, actor_id, prior_state, new_state, reason, correlation_id, occurred_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&transition.id)
        .bind(&transition.task_id)
        .bind(&transition.actor_id)
        .bind(&transition.prior_state)
        .bind(&transition.new_state)
        .bind(&transition.reason)
        .bind(&transition.correlation_id)
        .bind(transition.occurred_at_ms)
        .execute(&mut *connection)
        .await?;
    }

    sqlx::query(
        "INSERT INTO command_idempotency (actor_id, idempotency_key, request_hash, outcome_json, committed_at_ms, expires_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(&batch.actor_id)
    .bind(&batch.idempotency_key)
    .bind(&batch.request_hash)
    .bind(&batch.outcome_json)
    .bind(batch.committed_at_ms)
    .bind(batch.expires_at_ms)
    .execute(&mut *connection)
    .await?;

    Ok(CommitResult::Applied {
        outcome_json: batch.outcome_json.clone(),
    })
}

mod file_broker;
pub use file_broker::{
    FileBrokerBinding, FileBrokerLaunchBinding, FileBrokerOwner, FileBrokerRecoveryCandidate,
    FileBrokerStatus, UnreleasedBrokerLaunchProof,
};
#[cfg(test)]
mod file_broker_barrier_tests;
#[cfg(test)]
mod file_broker_crash_tests;
#[cfg(test)]
mod file_broker_tests;

#[cfg(test)]
mod file_broker_owner_tests;
