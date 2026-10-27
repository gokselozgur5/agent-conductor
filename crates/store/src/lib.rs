//! SQLite persistence for Agent Conductor.
//!
//! This crate stores outcomes produced by the domain layer. It deliberately does
//! not decide whether a lifecycle transition is valid.

use std::{path::Path, str::FromStr, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use sqlx::{
    Connection, Executor, SqliteConnection, SqlitePool,
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use thiserror::Error;
use tokio::sync::Mutex;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

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
    #[error("invalid database path")]
    InvalidPath,
}

#[derive(Clone)]
pub struct Store {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    writer: Mutex<SqliteConnection>,
    readers: SqlitePool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskMutation {
    pub task_id: String,
    pub expected_version: i64,
    pub new_version: i64,
    pub state: String,
    pub updated_at_ms: i64,
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
pub struct NewTask<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
    pub objective: &'a str,
    pub state: &'a str,
    pub priority: i64,
    pub authority_json: &'a str,
    pub created_at_ms: i64,
}

impl Store {
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
            }),
        })
    }

    pub fn readers(&self) -> &SqlitePool {
        &self.inner.readers
    }

    pub async fn create_project(&self, project: NewProject<'_>) -> Result<(), StoreError> {
        let mut writer = self.inner.writer.lock().await;
        writer.execute("BEGIN IMMEDIATE").await?;
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
        .map_err(StoreError::from);
        finish_transaction(&mut writer, result).await
    }

    pub async fn create_task(&self, task: NewTask<'_>) -> Result<(), StoreError> {
        let mut writer = self.inner.writer.lock().await;
        writer.execute("BEGIN IMMEDIATE").await?;
        let result = sqlx::query(
            "INSERT INTO tasks (id, project_id, objective, state, priority, version, authority_json, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)",
        )
        .bind(task.id)
        .bind(task.project_id)
        .bind(task.objective)
        .bind(task.state)
        .bind(task.priority)
        .bind(task.authority_json)
        .bind(task.created_at_ms)
        .bind(task.created_at_ms)
        .execute(&mut *writer)
        .await
        .map(|_| ())
        .map_err(StoreError::from);
        finish_transaction(&mut writer, result).await
    }

    pub async fn persist(&self, batch: &CommitBatch) -> Result<CommitResult, StoreError> {
        let mut writer = self.inner.writer.lock().await;
        writer.execute("BEGIN IMMEDIATE").await?;

        let result = persist_in_transaction(&mut writer, batch).await;
        finish_transaction(&mut writer, result).await
    }
}

async fn finish_transaction<T>(
    connection: &mut SqliteConnection,
    result: Result<T, StoreError>,
) -> Result<T, StoreError> {
    match result {
        Ok(value) => match connection.execute("COMMIT").await {
            Ok(_) => Ok(value),
            Err(error) => {
                let _ = connection.execute("ROLLBACK").await;
                Err(StoreError::Database(error))
            }
        },
        Err(error) => {
            let _ = connection.execute("ROLLBACK").await;
            Err(error)
        }
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

    if let Some(task) = &batch.task {
        let result = sqlx::query(
            "UPDATE tasks SET state = ?, version = ?, updated_at_ms = ? WHERE id = ? AND version = ?",
        )
        .bind(&task.state)
        .bind(task.new_version)
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
