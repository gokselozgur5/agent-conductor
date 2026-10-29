//! Native host witnesses for legacy ownership; no process receipt is invented.
use crate::{AuxiliaryPreparation, Store, StoreError};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, Row, SqliteConnection};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuxiliaryHostObservation {
    pub authority_id: String,
    pub generation: i64,
    pub platform: String,
    pub host_id: String,
    pub boot_id: String,
}
impl AuxiliaryHostObservation {
    fn validate(&self) -> Result<(), StoreError> {
        if self.generation <= 0
            || !matches!(self.platform.as_str(), "macos" | "linux")
            || [&self.authority_id, &self.host_id, &self.boot_id]
                .iter()
                .any(|v| {
                    v.len() != 64
                        || !v
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
        {
            return Err(invalid());
        }
        Ok(())
    }
}
fn invalid() -> StoreError {
    StoreError::Integrity("legacy auxiliary host evidence refused".into())
}

impl Store {
    /// Whether a bounded reconciliation pass has work in this generation.
    /// # Errors
    /// Returns database errors without treating missing evidence as settlement.
    pub async fn needs_legacy_host_observation(&self, generation: i64) -> Result<bool, StoreError> {
        Ok(sqlx::query_scalar::<_,i64>("SELECT EXISTS(SELECT 1 FROM legacy_auxiliary_ownership l LEFT JOIN legacy_auxiliary_host_witness w ON w.task_id=l.task_id WHERE w.task_id IS NULL OR w.checked_generation < ?)")
            .bind(generation).fetch_one(&self.inner.readers).await? != 0)
    }

    /// Records trusted native evidence, or resolves an exact prior witness after
    /// a same-host boot change. The caller must obtain the host identity natively
    /// while owning the runtime authority; this is not a public proof endpoint.
    /// # Errors
    /// Refuses invalid/stale authority and rolls back the entire bounded batch on
    /// database or audit failure. Contradictory individual witnesses stay blocked.
    pub async fn reconcile_legacy_auxiliary_host(
        &self,
        host: &AuxiliaryHostObservation,
        now_ms: i64,
    ) -> Result<usize, StoreError> {
        host.validate()?;
        let mut connection = self.inner.writer.lock().await;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        let owner:i64=sqlx::query_scalar("SELECT COUNT(*) FROM recovery_authority a JOIN runtime_metadata r ON r.singleton=a.singleton WHERE a.singleton=1 AND a.authority_id=? AND r.boot_generation=?")
            .bind(&host.authority_id).bind(host.generation).fetch_one(&mut *tx).await?;
        if owner != 1 {
            return Err(invalid());
        }
        let rows:Vec<(String,String,i64)>=sqlx::query_as("SELECT l.task_id,l.reason,l.recorded_boot_generation FROM legacy_auxiliary_ownership l LEFT JOIN legacy_auxiliary_host_witness w ON w.task_id=l.task_id WHERE w.task_id IS NULL OR w.checked_generation < ? ORDER BY COALESCE(w.checked_generation,0),l.task_id LIMIT 128")
            .bind(host.generation).fetch_all(&mut *tx).await?;
        let mut settled = 0;
        for (task, reason, marker_generation) in rows {
            settled += usize::from(
                reconcile_one(&mut tx, host, now_ms, &task, &reason, marker_generation).await?,
            );
        }
        tx.commit().await?;
        Ok(settled)
    }
}

async fn ownership(
    db: &mut SqliteConnection,
    task: &str,
    reason: &str,
    generation: i64,
) -> Result<Option<String>, StoreError> {
    if reason == "verification_without_invocation" {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM auxiliary_invocations WHERE task_id=?")
                .bind(task)
                .fetch_one(db)
                .await?;
        return Ok((count == 0).then(|| "null".into()));
    }
    if reason != "unbound_pending_invocation" {
        return Ok(None);
    }
    let row=sqlx::query("SELECT invocation_id,execution_token,CASE WHEN length(preparation_json)<=16384 THEN preparation_json ELSE NULL END AS preparation_json,boot_generation,(launch_binding_json IS NOT NULL OR process_json IS NOT NULL OR settlement_json IS NOT NULL) AS has_evidence,release_recorded FROM auxiliary_invocations WHERE task_id=? AND state='pending'").bind(task).fetch_optional(db).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let Some(raw) = row.try_get::<Option<String>, _>("preparation_json")? else {
        return Ok(None);
    };
    let Ok(input) = serde_json::from_str::<AuxiliaryPreparation>(&raw) else {
        return Ok(None);
    };
    if input.validate().is_err()
        || input.task_id != task
        || input.invocation_id != row.try_get::<String, _>("invocation_id")?
        || input.execution_token != row.try_get::<String, _>("execution_token")?
        || input.boot_generation != row.try_get::<i64, _>("boot_generation")?
        || input.boot_generation > generation
        || row.try_get::<i64, _>("release_recorded")? != 0
        || row.try_get::<i64, _>("has_evidence")? != 0
    {
        return Ok(None);
    }
    Ok(Some(
        serde_json::json!({"invocation_id":input.invocation_id,"preparation_json":raw}).to_string(),
    ))
}

async fn audit(
    db: &mut SqliteConnection,
    task: &str,
    kind: &str,
    payload: &str,
    now_ms: i64,
) -> Result<(), StoreError> {
    let id = uuid::Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO audit_events(id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES(?,?,'conductor-runtime',?,?,?,?)")
        .bind(&id).bind(task).bind(kind).bind(&id).bind(payload).bind(now_ms).execute(db).await?;
    Ok(())
}

async fn reconcile_one(
    db: &mut SqliteConnection,
    host: &AuxiliaryHostObservation,
    now_ms: i64,
    task: &str,
    reason: &str,
    marker_generation: i64,
) -> Result<bool, StoreError> {
    let current = ownership(db, task, reason, marker_generation).await?;
    let old = sqlx::query("SELECT substr(authority_id,1,65) AS authority_id,substr(platform,1,16) AS platform,substr(host_id,1,65) AS host_id,substr(boot_id,1,65) AS boot_id,observed_generation,substr(reason,1,64) AS reason,marker_generation,substr(status,1,32) AS status,CASE WHEN length(ownership_json)<=32768 THEN ownership_json ELSE 'false' END AS ownership_json FROM legacy_auxiliary_host_witness WHERE task_id=?")
        .bind(task)
        .fetch_optional(&mut *db)
        .await?;
    let Some(old) = old else {
        let status = if current.is_some() && marker_generation <= host.generation {
            "observed"
        } else {
            "contradiction"
        };
        let payload=serde_json::json!({"host":host,"reason":reason,"marker_generation":marker_generation,"status":status}).to_string();
        sqlx::query("INSERT INTO legacy_auxiliary_host_witness(task_id,reason,marker_generation,authority_id,platform,host_id,boot_id,observed_generation,checked_generation,ownership_json,status) VALUES(?,?,?,?,?,?,?,?,?,?,?)")
            .bind(task).bind(reason).bind(marker_generation).bind(&host.authority_id).bind(&host.platform).bind(&host.host_id).bind(&host.boot_id).bind(host.generation).bind(host.generation).bind(current.unwrap_or_else(||"null".into())).bind(status).execute(&mut *db).await?;
        audit(db, task, "auxiliary.legacy_host_observed", &payload, now_ms).await?;
        return Ok(false);
    };
    let previous = AuxiliaryHostObservation {
        authority_id: old.try_get("authority_id")?,
        generation: old.try_get("observed_generation")?,
        platform: old.try_get("platform")?,
        host_id: old.try_get("host_id")?,
        boot_id: old.try_get("boot_id")?,
    };
    let consistent = previous.validate().is_ok()
        && previous.authority_id == host.authority_id
        && previous.platform == host.platform
        && previous.host_id == host.host_id
        && old.try_get::<String, _>("reason")? == reason
        && old.try_get::<i64, _>("marker_generation")? == marker_generation
        && marker_generation <= previous.generation
        && previous.generation < host.generation
        && old.try_get::<String, _>("status")? == "observed"
        && current.as_deref() == Some(old.try_get::<String, _>("ownership_json")?.as_str());
    let resolved = consistent && previous.boot_id != host.boot_id;
    let status = if resolved {
        "settled"
    } else if consistent {
        "observed"
    } else {
        "contradiction"
    };
    sqlx::query(
        "UPDATE legacy_auxiliary_host_witness SET checked_generation=?,status=? WHERE task_id=?",
    )
    .bind(host.generation)
    .bind(status)
    .bind(task)
    .execute(&mut *db)
    .await?;
    if !resolved {
        return Ok(false);
    }
    let proof=serde_json::json!({"observation":"native_host_boot_transition","previous":previous,"current":host,"reason":reason,"marker_generation":marker_generation}).to_string();
    audit(db, task, "auxiliary.legacy_host_settled", &proof, now_ms).await?;
    sqlx::query("UPDATE auxiliary_invocations SET state='settled',settled_at_ms=?,settlement_json=? WHERE task_id=? AND state='pending'")
        .bind(now_ms).bind(&proof).bind(task).execute(&mut *db).await?;
    sqlx::query("UPDATE legacy_auxiliary_host_witness SET settlement_json=? WHERE task_id=?")
        .bind(&proof)
        .bind(task)
        .execute(&mut *db)
        .await?;
    sqlx::query("DELETE FROM legacy_auxiliary_ownership WHERE task_id=?")
        .bind(task)
        .execute(db)
        .await?;
    Ok(true)
}
