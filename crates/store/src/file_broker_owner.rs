//! Parent-observed broker ownership and receipt-bound closure.
use super::{audit, bound, digest, invalid, live};
use crate::{EffectAudit, ExecutionProcess, FileBrokerBinding, Store, StoreError};
use serde::{Deserialize, Serialize};
use sqlx::Connection;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileBrokerOwner {
    pub launch_sha256: String,
    pub receipt_root_identity_sha256: String,
    pub process: ExecutionProcess,
    /// Canonical private directory locator; absent on older records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_root: Option<String>,
}
impl FileBrokerOwner {
    pub(super) fn encoded(&self) -> Result<String, StoreError> {
        if !digest(&self.launch_sha256) || !digest(&self.receipt_root_identity_sha256) {
            return Err(invalid());
        }
        self.process.validate()?;
        if self.receipt_root.as_ref().is_some_and(|path| {
            path.is_empty()
                || path.len() > 4096
                || path.chars().any(char::is_control)
                || !std::path::Path::new(path).is_absolute()
        }) {
            return Err(invalid());
        }
        let encoded = serde_json::to_string(self).map_err(|_| invalid())?;
        if encoded.len() > 4096 {
            return Err(invalid());
        }
        Ok(encoded)
    }
}
impl Store {
    /// Persist independently observed process identity before releasing a broker.
    /// Only a fresh commit grants release; replay returns false, never permission.
    /// # Errors
    /// Refuses stale ownership, changed identity, closed brokers or prior operations.
    pub async fn authorize_file_broker_release(
        &self,
        binding: &FileBrokerBinding,
        owner: &FileBrokerOwner,
        event: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        let encoded_binding = binding.encoded()?;
        let encoded = owner.encoded()?;
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        live(&mut tx, binding).await?;
        if bound(&mut tx, binding, &encoded_binding).await? != "open" {
            return Err(invalid());
        }
        let existing: Option<String> =
            sqlx::query_scalar("SELECT owner_json FROM file_broker_owners WHERE invocation_id=?")
                .bind(&binding.invocation_id)
                .fetch_optional(&mut *tx)
                .await?;
        if let Some(existing) = existing {
            if existing != encoded {
                return Err(StoreError::IdempotencyCollision);
            }
            tx.commit().await?;
            return Ok(false);
        }
        let operations: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM file_operations WHERE invocation_id=?")
                .bind(&binding.invocation_id)
                .fetch_one(&mut *tx)
                .await?;
        if operations != 0 {
            return Err(invalid());
        }
        sqlx::query("INSERT INTO file_broker_owners(invocation_id,owner_json,authorized_at_ms) VALUES(?,?,?)")
            .bind(&binding.invocation_id).bind(encoded).bind(event.now_ms).execute(&mut *tx).await?;
        audit(&mut tx, binding, "file_broker.release_authorized", 0, event).await?;
        tx.commit().await?;
        Ok(true)
    }
    /// Read a bounded, strictly decoded owner for this exact binding.
    /// # Errors
    /// Refuses missing, malformed or substituted persisted records.
    pub async fn file_broker_owner(
        &self,
        binding: &FileBrokerBinding,
    ) -> Result<FileBrokerOwner, StoreError> {
        let encoded = binding.encoded()?;
        let value: Option<String> = sqlx::query_scalar("SELECT CASE WHEN length(o.owner_json)<=4096 THEN o.owner_json ELSE '' END FROM file_broker_owners o JOIN file_brokers b USING(invocation_id) WHERE b.invocation_id=? AND b.binding_json=?")
            .bind(&binding.invocation_id).bind(encoded).fetch_optional(&self.inner.readers).await?;
        let value = value.ok_or_else(invalid)?;
        let owner: FileBrokerOwner = serde_json::from_str(&value).map_err(|_| invalid())?;
        if owner.encoded()? != value {
            return Err(invalid());
        }
        Ok(owner)
    }
    /// Close only after the trusted service verifies the independent receipt.
    /// The digest binds that exact receipt; this does not fence the execution.
    /// # Errors
    /// Refuses substituted owner/receipt, stale authority or pending operations.
    pub async fn close_owned_file_broker(
        &self,
        binding: &FileBrokerBinding,
        owner: &FileBrokerOwner,
        completed: i64,
        shutdown_sha256: &str,
        event: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        if !digest(shutdown_sha256) {
            return Err(invalid());
        }
        self.close_file_broker_inner(
            binding,
            completed,
            Some((owner, shutdown_sha256)),
            None,
            event,
        )
        .await
    }
}

impl Store {
    /// Reconcile an older broker under the currently reserved daemon generation.
    /// Requires independently verified receipt evidence; never authorizes release.
    /// # Errors
    /// Refuses current/foreign/stale generations, substituted owners and pending effects.
    pub async fn recover_owned_file_broker(
        &self,
        binding: &FileBrokerBinding,
        owner: &FileBrokerOwner,
        completed: i64,
        shutdown_sha256: &str,
        generation: i64,
        event: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        if !digest(shutdown_sha256) {
            return Err(invalid());
        }
        self.close_file_broker_inner(
            binding,
            completed,
            Some((owner, shutdown_sha256)),
            Some(generation),
            event,
        )
        .await
    }
}

pub(super) async fn recovery_audit(
    db: &mut sqlx::SqliteConnection,
    binding: &FileBrokerBinding,
    completed: i64,
    generation: i64,
    event: EffectAudit<'_>,
) -> Result<(), StoreError> {
    sqlx::query("INSERT INTO audit_events(id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES(?,?,?,'file_broker.recovered',?,?,?)")
        .bind(event.id).bind(&binding.task_id).bind(event.actor_id).bind(event.correlation_id)
        .bind(serde_json::json!({"invocation_id":binding.invocation_id,"sequence":completed,"origin_generation":binding.boot_generation,"recovery_generation":generation}).to_string())
        .bind(event.now_ms).execute(db).await?;
    Ok(())
}

/// One independently decoded pending row; invalid rows remain unresolved.
pub struct FileBrokerRecoveryCandidate {
    pub invocation_id: String,
    pub binding: Result<FileBrokerBinding, StoreError>,
}
impl Store {
    /// Stable bounded discovery; one malformed binding does not hide later rows.
    /// Ownerless registrations are retained for separate prelaunch recovery.
    /// # Errors
    /// Returns database read errors; binding JSON is bounded before decoding.
    pub async fn pending_file_broker_page(
        &self,
        after: Option<&str>,
    ) -> Result<Vec<FileBrokerRecoveryCandidate>, StoreError> {
        let rows: Vec<(String, String, String)> = sqlx::query_as("SELECT b.invocation_id,b.execution_token,CASE WHEN length(b.binding_json)<=4096 THEN b.binding_json ELSE '' END FROM file_brokers b JOIN file_broker_owners o USING(invocation_id) WHERE b.state='open' AND (? IS NULL OR b.invocation_id>?) ORDER BY b.invocation_id LIMIT 128")
            .bind(after).bind(after).fetch_all(&self.inner.readers).await?;
        Ok(rows
            .into_iter()
            .map(|(invocation_id, execution_token, encoded)| {
                let binding = (|| {
                    let binding: FileBrokerBinding =
                        serde_json::from_str(&encoded).map_err(|_| invalid())?;
                    if binding.invocation_id != invocation_id
                        || binding.execution_token != execution_token
                        || binding.encoded()? != encoded
                    {
                        return Err(invalid());
                    }
                    Ok(binding)
                })();
                FileBrokerRecoveryCandidate {
                    invocation_id,
                    binding,
                }
            })
            .collect())
    }
}
