//! Deterministic failure harnesses for the Phase 1 control core.
//!
//! The fake effect inventory is deliberately stored outside the Conductor
//! database. It models an external system that can commit independently of the
//! transactional outbox and survive a daemon crash.

use std::{path::Path, str::FromStr, time::Duration};

use agent_conductor_store::{EffectAudit, EffectRecord, EffectStatus, Store, StoreError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{
    Connection, Executor, Row, SqliteConnection,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
};
use thiserror::Error;
use tokio::sync::Mutex;

/// An external resource tagged with the stable effect token that created it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FakeResource {
    pub token: String,
    pub effect_kind: String,
    pub request_json: String,
    pub resource_id: String,
    pub created_at_ms: i64,
}

/// The action a caller must take when reconciliation detects unsafe state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconcileDirective {
    Settled(FakeResource),
    AlreadySettled(FakeResource),
    RetryAfter {
        deadline_ms: i64,
    },
    BlockTask {
        task_id: Option<String>,
        reason: String,
    },
}

/// Named boundaries used by subprocess tests to kill the runner exactly where
/// the control database and external inventory may diverge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrashPoint {
    Never,
    BeforeClaim,
    AfterClaim,
    AfterExternalEffect,
    AfterObservation,
    AfterDone,
}

impl CrashPoint {
    fn hit(self, current: Self) {
        if self == current {
            std::process::abort();
        }
    }
}

#[derive(Debug, Error)]
pub enum FakeEffectError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid fake inventory path")]
    InvalidPath,
    #[error("fake inventory contradiction for token {token}: {detail}")]
    Contradiction { token: String, detail: String },
}

/// Durable fake external system. Token uniqueness makes execution idempotent.
pub struct FakeInventory {
    writer: Mutex<SqliteConnection>,
}

impl FakeInventory {
    /// Opens or creates the external inventory.
    ///
    /// # Errors
    /// Returns an error when the path or `SQLite` database is invalid.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, FakeEffectError> {
        let path = path.as_ref().to_str().ok_or(FakeEffectError::InvalidPath)?;
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{path}"))?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5));
        let mut writer = SqliteConnection::connect_with(&options).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS fake_resources (token TEXT PRIMARY KEY, effect_kind TEXT NOT NULL, request_json TEXT NOT NULL, resource_id TEXT NOT NULL UNIQUE, created_at_ms INTEGER NOT NULL)",
        )
        .execute(&mut writer)
        .await?;
        Ok(Self {
            writer: Mutex::new(writer),
        })
    }

    /// Returns a resource by stable effect token.
    ///
    /// # Errors
    /// Returns an error when `SQLite` cannot read the inventory.
    pub async fn resource(&self, token: &str) -> Result<Option<FakeResource>, FakeEffectError> {
        let mut writer = self.writer.lock().await;
        let row = sqlx::query(
            "SELECT token, effect_kind, request_json, resource_id, created_at_ms FROM fake_resources WHERE token = ?",
        )
        .bind(token)
        .fetch_optional(&mut *writer)
        .await?;
        row.as_ref().map(resource_from_row).transpose()
    }

    /// Executes the fake effect once. Replays return the original identity.
    ///
    /// # Errors
    /// Rejects a token reused for a different kind or request.
    pub async fn execute(
        &self,
        effect: &EffectRecord,
        now_ms: i64,
    ) -> Result<FakeResource, FakeEffectError> {
        let mut writer = self.writer.lock().await;
        writer.execute("BEGIN IMMEDIATE").await?;
        let result = async {
            if let Some(row) = sqlx::query(
                "SELECT token, effect_kind, request_json, resource_id, created_at_ms FROM fake_resources WHERE token = ?",
            )
            .bind(&effect.token)
            .fetch_optional(&mut *writer)
            .await?
            {
                let existing = resource_from_row(&row)?;
                ensure_matches(effect, &existing)?;
                return Ok(existing);
            }
            let resource = FakeResource {
                token: effect.token.clone(),
                effect_kind: effect.effect_kind.clone(),
                request_json: effect.request_json.clone(),
                resource_id: resource_identity(&effect.token),
                created_at_ms: now_ms,
            };
            sqlx::query(
                "INSERT INTO fake_resources (token, effect_kind, request_json, resource_id, created_at_ms) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(&resource.token)
            .bind(&resource.effect_kind)
            .bind(&resource.request_json)
            .bind(&resource.resource_id)
            .bind(resource.created_at_ms)
            .execute(&mut *writer)
            .await?;
            Ok(resource)
        }
        .await;
        match result {
            Ok(resource) => {
                writer.execute("COMMIT").await?;
                Ok(resource)
            }
            Err(error) => {
                let _ = writer.execute("ROLLBACK").await;
                Err(error)
            }
        }
    }

    /// Inserts a deliberately inconsistent resource for an abuse regression.
    ///
    /// # Errors
    /// Returns an error when `SQLite` rejects the row.
    pub async fn inject(&self, resource: &FakeResource) -> Result<(), FakeEffectError> {
        let mut writer = self.writer.lock().await;
        sqlx::query(
            "INSERT INTO fake_resources (token, effect_kind, request_json, resource_id, created_at_ms) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&resource.token)
        .bind(&resource.effect_kind)
        .bind(&resource.request_json)
        .bind(&resource.resource_id)
        .bind(resource.created_at_ms)
        .execute(&mut *writer)
        .await?;
        Ok(())
    }
}

/// Reconciles committed intent against the independently durable inventory.
pub struct FakeEffectRunner {
    store: Store,
    inventory: FakeInventory,
    runner_id: String,
    claim_duration_ms: i64,
}

impl FakeEffectRunner {
    #[must_use]
    pub fn new(
        store: Store,
        inventory: FakeInventory,
        runner_id: impl Into<String>,
        claim_duration_ms: i64,
    ) -> Self {
        Self {
            store,
            inventory,
            runner_id: runner_id.into(),
            claim_duration_ms,
        }
    }

    /// Converges one effect without executing it more than once.
    ///
    /// # Errors
    /// Returns database errors and reports contradictions after durably
    /// abandoning the unsafe intent. The returned block directive is the typed
    /// input the control service must apply to the owning task.
    pub async fn reconcile(
        &self,
        token: &str,
        now_ms: i64,
        crash: CrashPoint,
    ) -> Result<ReconcileDirective, FakeEffectError> {
        crash.hit(CrashPoint::BeforeClaim);
        let Some(effect) = self.store.effect(token).await? else {
            return if self.inventory.resource(token).await?.is_some() {
                Ok(ReconcileDirective::BlockTask {
                    task_id: None,
                    reason: format!("unmatched tagged resource {token}"),
                })
            } else {
                Err(StoreError::EffectNotFound(token.to_owned()).into())
            };
        };

        if effect.status == EffectStatus::Abandoned {
            return Ok(ReconcileDirective::BlockTask {
                task_id: effect.task_id,
                reason: "effect was abandoned after an earlier contradiction".to_owned(),
            });
        }

        if let Some(resource) = self.inventory.resource(token).await?
            && let Err(error) = ensure_matches(&effect, &resource)
        {
            return if effect.status == EffectStatus::Done {
                Ok(ReconcileDirective::BlockTask {
                    task_id: effect.task_id,
                    reason: error.to_string(),
                })
            } else {
                self.abandon(&effect, now_ms, error.to_string()).await
            };
        }

        if effect.status == EffectStatus::Done {
            let Some(resource) = self.inventory.resource(token).await? else {
                return Ok(ReconcileDirective::BlockTask {
                    task_id: effect.task_id,
                    reason: "done intent has no tagged resource".to_owned(),
                });
            };
            return Ok(ReconcileDirective::AlreadySettled(resource));
        }

        let effect = self.claim_if_needed(effect, now_ms, crash).await?;
        if effect.status == EffectStatus::Observed
            && effect.runner_id.as_deref() != Some(&self.runner_id)
            && effect
                .claim_deadline_ms
                .is_some_and(|deadline| now_ms < deadline)
        {
            return Ok(ReconcileDirective::RetryAfter {
                deadline_ms: effect.claim_deadline_ms.unwrap_or(now_ms),
            });
        }
        if effect.status == EffectStatus::Claimed
            && effect.runner_id.as_deref() != Some(&self.runner_id)
        {
            return Ok(ReconcileDirective::RetryAfter {
                deadline_ms: effect.claim_deadline_ms.unwrap_or(now_ms),
            });
        }

        self.finish_claimed_effect(effect, now_ms, crash).await
    }

    async fn claim_if_needed(
        &self,
        mut effect: EffectRecord,
        now_ms: i64,
        crash: CrashPoint,
    ) -> Result<EffectRecord, FakeEffectError> {
        let token = effect.token.clone();
        if effect.status == EffectStatus::Pending
            || (effect.status == EffectStatus::Claimed
                && effect
                    .claim_deadline_ms
                    .is_some_and(|deadline| now_ms >= deadline))
        {
            let deadline = now_ms.checked_add(self.claim_duration_ms).ok_or_else(|| {
                FakeEffectError::Contradiction {
                    token: token.clone(),
                    detail: "claim deadline overflow".to_owned(),
                }
            })?;
            let audit_id = audit_id(&token, "claim", now_ms);
            let correlation_id = correlation_id(&token);
            effect = self
                .store
                .claim_effect(
                    &token,
                    &self.runner_id,
                    deadline,
                    EffectAudit {
                        id: &audit_id,
                        actor_id: &self.runner_id,
                        correlation_id: &correlation_id,
                        now_ms,
                    },
                )
                .await?;
            crash.hit(CrashPoint::AfterClaim);
        }
        Ok(effect)
    }

    async fn finish_claimed_effect(
        &self,
        mut effect: EffectRecord,
        now_ms: i64,
        crash: CrashPoint,
    ) -> Result<ReconcileDirective, FakeEffectError> {
        let token = effect.token.clone();
        if effect.status == EffectStatus::Claimed
            && !self
                .store
                .effect_is_executable(&token, &self.runner_id)
                .await?
        {
            return Ok(ReconcileDirective::BlockTask {
                task_id: effect.task_id,
                reason: "effect no longer belongs to a live task attempt".to_owned(),
            });
        }
        let resource = self.inventory.execute(&effect, now_ms).await?;
        crash.hit(CrashPoint::AfterExternalEffect);
        let observation = serde_json::to_string(&resource)?;

        if effect.status == EffectStatus::Claimed {
            let audit_id = audit_id(&token, "observe", now_ms);
            let correlation_id = correlation_id(&token);
            effect = self
                .store
                .observe_effect(
                    &token,
                    &self.runner_id,
                    &observation,
                    EffectAudit {
                        id: &audit_id,
                        actor_id: &self.runner_id,
                        correlation_id: &correlation_id,
                        now_ms,
                    },
                )
                .await?;
            crash.hit(CrashPoint::AfterObservation);
        }

        if effect.status == EffectStatus::Observed {
            if effect.observation_json.as_deref() != Some(observation.as_str()) {
                return self
                    .abandon(
                        &effect,
                        now_ms,
                        "observation does not match inventory".to_owned(),
                    )
                    .await;
            }
            let audit_id = audit_id(&token, "done", now_ms);
            let correlation_id = correlation_id(&token);
            self.store
                .complete_effect(
                    &token,
                    &self.runner_id,
                    EffectAudit {
                        id: &audit_id,
                        actor_id: &self.runner_id,
                        correlation_id: &correlation_id,
                        now_ms,
                    },
                )
                .await?;
            crash.hit(CrashPoint::AfterDone);
        }

        Ok(ReconcileDirective::Settled(resource))
    }

    async fn abandon(
        &self,
        effect: &EffectRecord,
        now_ms: i64,
        reason: String,
    ) -> Result<ReconcileDirective, FakeEffectError> {
        let payload = serde_json::json!({"reason": reason}).to_string();
        let audit_id = audit_id(&effect.token, "abandon", now_ms);
        let correlation_id = correlation_id(&effect.token);
        self.store
            .abandon_effect(
                &effect.token,
                &payload,
                EffectAudit {
                    id: &audit_id,
                    actor_id: &self.runner_id,
                    correlation_id: &correlation_id,
                    now_ms,
                },
            )
            .await?;
        Ok(ReconcileDirective::BlockTask {
            task_id: effect.task_id.clone(),
            reason,
        })
    }
}

fn audit_id(token: &str, step: &str, now_ms: i64) -> String {
    format!("fake-effect:{token}:{step}:{now_ms}")
}

fn correlation_id(token: &str) -> String {
    format!("fake-reconcile:{token}")
}

fn resource_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<FakeResource, FakeEffectError> {
    Ok(FakeResource {
        token: row.try_get("token")?,
        effect_kind: row.try_get("effect_kind")?,
        request_json: row.try_get("request_json")?,
        resource_id: row.try_get("resource_id")?,
        created_at_ms: row.try_get("created_at_ms")?,
    })
}

fn ensure_matches(effect: &EffectRecord, resource: &FakeResource) -> Result<(), FakeEffectError> {
    if effect.effect_kind != resource.effect_kind || effect.request_json != resource.request_json {
        return Err(FakeEffectError::Contradiction {
            token: effect.token.clone(),
            detail: "tagged resource does not match committed intent".to_owned(),
        });
    }
    Ok(())
}

fn resource_identity(token: &str) -> String {
    let digest = Sha256::digest(format!("agent-conductor.fake-resource.v1\0{token}").as_bytes());
    format!("fake:{digest:x}")
}
