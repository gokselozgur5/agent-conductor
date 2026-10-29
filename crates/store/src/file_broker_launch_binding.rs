use super::{FileBrokerBinding, FileBrokerLaunchBinding, invalid};
use crate::{Store, StoreError};

impl Store {
    /// Read the exact persisted launch without authorizing process creation.
    /// # Errors
    /// Rejects absent, malformed, noncanonical or substituted launch metadata.
    pub async fn file_broker_launch(
        &self,
        binding: &FileBrokerBinding,
    ) -> Result<FileBrokerLaunchBinding, StoreError> {
        let value: Option<String> = sqlx::query_scalar("SELECT CASE WHEN length(launch_json)<=16384 THEN launch_json ELSE '' END FROM file_broker_launches WHERE invocation_id=? AND execution_token=? AND binding_json=?")
            .bind(&binding.invocation_id).bind(&binding.execution_token).bind(binding.encoded()?).fetch_optional(&self.inner.readers).await?;
        let value = value.ok_or_else(invalid)?;
        let launch: FileBrokerLaunchBinding =
            serde_json::from_str(&value).map_err(|_| invalid())?;
        launch.validate()?;
        if serde_json::to_string(&launch).map_err(|_| invalid())? != value {
            return Err(invalid());
        }
        Ok(launch)
    }
}

pub(super) async fn registered_binding(
    db: &mut sqlx::SqliteConnection,
    binding: &FileBrokerBinding,
) -> Result<(), StoreError> {
    let saved: Option<(String, String)> = sqlx::query_as(
        "SELECT binding_json,state FROM file_broker_launches WHERE execution_token=?",
    )
    .bind(&binding.execution_token)
    .fetch_optional(db)
    .await?;
    if let Some((encoded, state)) = saved
        && (encoded != binding.encoded()? || state != "open")
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) async fn authorize_owner(
    db: &mut sqlx::SqliteConnection,
    binding: &FileBrokerBinding,
    owner: &crate::FileBrokerOwner,
) -> Result<(), StoreError> {
    let saved: Option<(String, String)> =
        sqlx::query_as("SELECT launch_json,state FROM file_broker_launches WHERE invocation_id=?")
            .bind(&binding.invocation_id)
            .fetch_optional(&mut *db)
            .await?;
    if let Some((saved, state)) = saved {
        let launch: FileBrokerLaunchBinding =
            serde_json::from_str(&saved).map_err(|_| invalid())?;
        launch.validate()?;
        if state != "open"
            || serde_json::to_string(&launch).map_err(|_| invalid())? != saved
            || launch.launch_sha256 != owner.launch_sha256
            || launch.receipt_root_identity_sha256 != owner.receipt_root_identity_sha256
            || owner.receipt_root.as_deref() != Some(launch.receipt_root.as_str())
            || launch.platform != owner.process.platform
            || launch.host_id != owner.process.host_id
            || launch.boot_id != owner.process.boot_id
        {
            return Err(invalid());
        }
        let acknowledged: Option<String> =
            sqlx::query_scalar("SELECT process_json FROM executions WHERE effect_token=?")
                .bind(&binding.execution_token)
                .fetch_one(db)
                .await?;
        if acknowledged != Some(serde_json::to_string(&owner.process).map_err(|_| invalid())?) {
            return Err(invalid());
        }
    }
    Ok(())
}

pub(super) async fn close_preparation(
    db: &mut sqlx::SqliteConnection,
    binding: &FileBrokerBinding,
    broker_state: &str,
    receipt: Option<&str>,
) -> Result<(), StoreError> {
    let saved: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT binding_json,state,shutdown_sha256 FROM file_broker_launches WHERE invocation_id=?",
    )
    .bind(&binding.invocation_id)
    .fetch_optional(&mut *db)
    .await?;
    if let Some((encoded, state, prior)) = saved {
        let receipt = receipt.ok_or_else(invalid)?;
        if encoded != binding.encoded()?
            || state != broker_state
            || prior.as_deref().is_some_and(|old| old != receipt)
            || (state == "closed" && prior.is_none())
        {
            return Err(invalid());
        }
        sqlx::query("UPDATE file_broker_launches SET state='closed',shutdown_sha256=? WHERE invocation_id=?")
            .bind(receipt).bind(&binding.invocation_id).execute(db).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::file_broker_tests::{audit, fixture};
    use crate::transaction_cancellation::snapshot;
    #[tokio::test]
    async fn registration_refuses_substituted_or_closed_preparation() {
        let (_root, store, binding) = fixture().await;
        sqlx::query("INSERT INTO file_broker_launches VALUES(?,?,'{}','{}','open',100,NULL)")
            .bind(&binding.invocation_id)
            .bind(&binding.execution_token)
            .execute(&mut *store.inner.writer.lock().await)
            .await
            .unwrap();
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        assert!(
            store
                .register_file_broker(&binding, audit("launch-mismatch"))
                .await
                .is_err()
        );
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            before
        );
        sqlx::query(
            "UPDATE file_broker_launches SET binding_json=?,state='closed',shutdown_sha256=?",
        )
        .bind(binding.encoded().unwrap())
        .bind("a".repeat(64))
        .execute(&mut *store.inner.writer.lock().await)
        .await
        .unwrap();
        assert!(
            store
                .register_file_broker(&binding, audit("launch-closed"))
                .await
                .is_err()
        );
        sqlx::query("UPDATE file_broker_launches SET state='open',shutdown_sha256=NULL")
            .execute(&mut *store.inner.writer.lock().await)
            .await
            .unwrap();
        assert!(
            store
                .register_file_broker(&binding, audit("launch-exact"))
                .await
                .unwrap()
        );
        assert!(store.file_broker_launch(&binding).await.is_err());
    }
}
