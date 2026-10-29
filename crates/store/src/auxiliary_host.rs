//! Host-transition settlement never stands in for a successful check or verdict.
use crate::{
    AuxiliaryHostObservation, AuxiliaryLaunchBinding, AuxiliaryPreparation, EffectAudit,
    ExecutionProcess, Store, StoreError,
};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, Row, SqliteConnection};

fn invalid() -> StoreError {
    StoreError::Integrity("auxiliary native host evidence refused".into())
}
pub(crate) async fn require_owner(
    db: &mut SqliteConnection,
    host: &AuxiliaryHostObservation,
) -> Result<(), StoreError> {
    host.validate()?;
    let n:i64=sqlx::query_scalar("SELECT COUNT(*) FROM recovery_authority a JOIN runtime_metadata r ON r.singleton=a.singleton WHERE a.singleton=1 AND a.authority_id=? AND r.boot_generation=?")
        .bind(&host.authority_id).bind(host.generation).fetch_one(db).await?;
    if n != 1 {
        return Err(invalid());
    }
    Ok(())
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ownership {
    preparation: String,
    binding: Option<String>,
    process: Option<String>,
    released: bool,
}
impl Ownership {
    fn validate(
        &self,
        current: &AuxiliaryHostObservation,
    ) -> Result<Option<ExecutionProcess>, StoreError> {
        let binding: Option<AuxiliaryLaunchBinding> = self
            .binding
            .as_ref()
            .map(|s| serde_json::from_str(s).map_err(|_| invalid()))
            .transpose()?;
        if let Some(b) = &binding {
            b.validate()?;
            if b.authority_id != current.authority_id {
                return Err(invalid());
            }
        }
        let process: Option<ExecutionProcess> = self
            .process
            .as_ref()
            .map(|s| serde_json::from_str(s).map_err(|_| invalid()))
            .transpose()?;
        if let Some(p) = &process {
            p.validate()?;
            if binding.is_none()
                || !self.released
                || p.platform != current.platform
                || p.host_id != current.host_id
            {
                return Err(invalid());
            }
        } else if self.released {
            return Err(invalid());
        }
        Ok(process)
    }
}
struct Evidence {
    fresh: bool,
    kind: String,
    host: AuxiliaryHostObservation,
    ownership: Option<String>,
}
async fn evidence(
    db: &mut SqliteConnection,
    input: &AuxiliaryPreparation,
    current: &AuxiliaryHostObservation,
    scope: &Ownership,
) -> Result<Evidence, StoreError> {
    let row=sqlx::query("SELECT kind,CASE WHEN length(host_json)<=4096 THEN host_json ELSE 'oversized' END AS host_json,CASE WHEN ownership_json IS NULL OR length(ownership_json)<=65536 THEN ownership_json ELSE 'oversized' END AS ownership_json FROM auxiliary_host_evidence WHERE invocation_id=?")
        .bind(&input.invocation_id).fetch_optional(&mut *db).await?;
    if let Some(row) = row {
        return Ok(Evidence {
            fresh: false,
            kind: row.try_get("kind")?,
            host: serde_json::from_str(&row.try_get::<String, _>("host_json")?)
                .map_err(|_| invalid())?,
            ownership: row.try_get("ownership_json")?,
        });
    }
    let process = scope.validate(current)?;
    let (kind, host) = if let Some(p) = process {
        (
            "recorded_process",
            AuxiliaryHostObservation {
                authority_id: current.authority_id.clone(),
                generation: input.boot_generation,
                platform: p.platform,
                host_id: p.host_id,
                boot_id: p.boot_id,
            },
        )
    } else {
        ("recovery_observation", current.clone())
    };
    let ownership = serde_json::to_string(scope).map_err(|_| invalid())?;
    sqlx::query("INSERT INTO auxiliary_host_evidence(invocation_id,kind,host_json,ownership_json) VALUES(?,?,?,?)")
        .bind(&input.invocation_id).bind(kind).bind(serde_json::to_string(&host).map_err(|_|invalid())?).bind(&ownership).execute(db).await?;
    Ok(Evidence {
        fresh: true,
        kind: kind.into(),
        host,
        ownership: Some(ownership),
    })
}
async fn pending_scope(
    db: &mut SqliteConnection,
    input: &AuxiliaryPreparation,
) -> Result<Option<Ownership>, StoreError> {
    let row=sqlx::query("SELECT task_id,execution_token,boot_generation,state,CASE WHEN length(preparation_json)<=16384 THEN preparation_json ELSE 'oversized' END AS preparation_json,CASE WHEN launch_binding_json IS NULL OR length(launch_binding_json)<=16384 THEN launch_binding_json ELSE 'oversized' END AS binding,CASE WHEN process_json IS NULL OR length(process_json)<=4096 THEN process_json ELSE 'oversized' END AS process,release_recorded,settlement_json IS NOT NULL AS has_settlement FROM auxiliary_invocations WHERE invocation_id=?")
        .bind(&input.invocation_id).fetch_optional(db).await?.ok_or_else(invalid)?;
    if row.try_get::<String, _>("task_id")? != input.task_id
        || row.try_get::<String, _>("execution_token")? != input.execution_token
        || row.try_get::<i64, _>("boot_generation")? != input.boot_generation
        || row.try_get::<String, _>("preparation_json")?
            != serde_json::to_string(input).map_err(|_| invalid())?
    {
        return Err(invalid());
    }
    if row.try_get::<String, _>("state")? == "settled" {
        return Ok(None);
    }
    if row.try_get::<i64, _>("has_settlement")? != 0 {
        return Err(invalid());
    }
    Ok(Some(Ownership {
        preparation: row.try_get("preparation_json")?,
        binding: row.try_get("binding")?,
        process: row.try_get("process")?,
        released: row.try_get::<i64, _>("release_recorded")? == 1,
    }))
}
impl Store {
    /// Uses a native boot transition to settle an old invocation without a result.
    /// Missing origin evidence with no acknowledged process records a first
    /// recovery witness only; same-boot observations never establish settlement.
    /// # Errors
    /// Refuses stale/foreign authority, changed scope and contradictory ownership.
    /// All evidence, audit and settlement writes roll back together on failure.
    pub async fn reconcile_auxiliary_host(
        &self,
        input: &AuxiliaryPreparation,
        current: &AuxiliaryHostObservation,
        audit: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        input.validate()?;
        let mut connection = self.inner.writer.lock().await;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        require_owner(&mut tx, current).await?;
        if input.boot_generation >= current.generation {
            return Err(invalid());
        }
        let marked: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM legacy_auxiliary_ownership WHERE task_id=?")
                .bind(&input.task_id)
                .fetch_one(&mut *tx)
                .await?;
        if marked != 0 {
            return Err(invalid());
        }
        let Some(scope) = pending_scope(&mut tx, input).await? else {
            tx.commit().await?;
            return Ok(false);
        };
        let process = scope.validate(current)?;
        let previous = evidence(&mut tx, input, current, &scope).await?;
        previous.host.validate()?;
        if previous.host.authority_id != current.authority_id
            || previous.host.platform != current.platform
            || previous.host.host_id != current.host_id
            || previous.host.generation < input.boot_generation
            || previous.host.generation > current.generation
        {
            return Err(invalid());
        }
        validate_evidence(&previous, input, &scope, process.as_ref())?;
        let settled = previous.host.boot_id != current.boot_id
            && previous.host.generation < current.generation;
        if settled {
            let proof=serde_json::json!({"observation":"native_host_boot_transition","invocation_id":input.invocation_id,"evidence_kind":previous.kind,"previous":previous.host,"current":current}).to_string();
            sqlx::query("UPDATE auxiliary_invocations SET state='settled',settled_at_ms=?,settlement_json=? WHERE invocation_id=? AND state='pending'")
                .bind(audit.now_ms).bind(&proof).bind(&input.invocation_id).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO audit_events(id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES(?,?,?,'auxiliary.host_settled',?,?,?)")
                .bind(audit.id).bind(&input.task_id).bind(audit.actor_id).bind(audit.correlation_id).bind(proof).bind(audit.now_ms).execute(&mut *tx).await?;
        }
        if previous.fresh && !settled {
            let payload=serde_json::json!({"invocation_id":input.invocation_id,"evidence_kind":previous.kind,"host":previous.host}).to_string();
            sqlx::query("INSERT INTO audit_events(id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES(?,?,?,'auxiliary.host_observed',?,?,?)")
                .bind(audit.id).bind(&input.task_id).bind(audit.actor_id).bind(audit.correlation_id).bind(payload).bind(audit.now_ms).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(settled)
    }
}
fn validate_evidence(
    previous: &Evidence,
    input: &AuxiliaryPreparation,
    scope: &Ownership,
    process: Option<&ExecutionProcess>,
) -> Result<(), StoreError> {
    match previous.kind.as_str() {
        "admission"
            if previous.host.generation == input.boot_generation
                && previous.ownership.is_none() =>
        {
            if process.is_some_and(|p| p.boot_id != previous.host.boot_id) {
                return Err(invalid());
            }
        }
        "recorded_process" | "recovery_observation"
            if previous.ownership.as_deref()
                == Some(
                    serde_json::to_string(scope)
                        .map_err(|_| invalid())?
                        .as_str(),
                ) =>
        {
            if previous.kind == "recorded_process"
                && (previous.host.generation != input.boot_generation
                    || process.is_none_or(|p| p.boot_id != previous.host.boot_id))
            {
                return Err(invalid());
            }
        }
        _ => return Err(invalid()),
    }
    Ok(())
}
