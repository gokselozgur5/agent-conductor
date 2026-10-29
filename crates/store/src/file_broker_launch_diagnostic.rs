use crate::{Store, StoreError};

impl Store {
    pub(super) async fn file_broker_launch_diagnostic(
        &self,
    ) -> Result<Option<crate::RuntimeInvariantViolation>, StoreError> {
        use sqlx::Row;
        let rows=sqlx::query("SELECT substr(p.invocation_id,1,256) AS invocation_id,substr(e.task_id,1,256) AS task_id,COUNT(*) OVER() AS total FROM file_broker_launches p JOIN executions e ON e.effect_token=p.execution_token LEFT JOIN tasks t ON t.id=e.task_id LEFT JOIN leases l ON l.attempt_id=e.attempt_id JOIN runtime_metadata r ON r.singleton=1 JOIN recovery_authority a ON a.singleton=1 WHERE p.state='open' AND (json_extract(p.binding_json,'$.boot_generation') IS NOT r.boot_generation OR json_extract(p.binding_json,'$.authority_id') IS NOT a.authority_id OR e.state NOT IN ('prepared','started') OR t.state NOT IN ('claimed','running') OR l.status IS NOT 'active') ORDER BY p.invocation_id LIMIT 128")
            .fetch_all(&self.inner.readers).await?;
        let Some(first) = rows.first() else {
            return Ok(None);
        };
        let total: i64 = first.try_get("total")?;
        let records:Result<Vec<_>,sqlx::Error>=rows.iter().map(|row|Ok(serde_json::json!({"invocation_id":row.try_get::<String,_>("invocation_id")?,"task_id":row.try_get::<String,_>("task_id")?,"stage":"launch_prepared"}))).collect();
        Ok(Some(crate::RuntimeInvariantViolation { code:"file_broker_recovery_pending".into(),summary:"broker launch ownership remains unresolved; preserve preparation and receipt evidence".into(),context_json:serde_json::json!({"count":total,"brokers":records?,"limit":128}).to_string() }))
    }
}

#[cfg(test)]
mod tests {
    use crate::file_broker_tests::fixture;
    use sqlx::Executor;
    #[tokio::test]
    async fn ownerless_preparation_is_visible_after_generation_changes() {
        let (_root, store, binding) = fixture().await;
        sqlx::query("INSERT INTO file_broker_launches VALUES(?,?,?,'{}','open',100,NULL)")
            .bind(&binding.invocation_id)
            .bind(&binding.execution_token)
            .bind(serde_json::to_string(&binding).unwrap())
            .execute(&mut *store.inner.writer.lock().await)
            .await
            .unwrap();
        assert!(
            store
                .execution_has_unresolved_file_broker(&binding.execution_token)
                .await
                .unwrap()
        );
        assert!(
            !store
                .execution_has_unresolved_file_broker("other")
                .await
                .unwrap()
        );
        assert!(
            store
                .file_broker_recovery_diagnostic()
                .await
                .unwrap()
                .is_none()
        );
        store
            .inner
            .writer
            .lock()
            .await
            .execute("UPDATE runtime_metadata SET boot_generation=boot_generation+1")
            .await
            .unwrap();
        let diagnostic = store
            .file_broker_recovery_diagnostic()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(diagnostic.code, "file_broker_recovery_pending");
        let context: serde_json::Value = serde_json::from_str(&diagnostic.context_json).unwrap();
        assert_eq!(context["brokers"][0]["stage"], "launch_prepared");
        assert_eq!(
            context["brokers"][0]["invocation_id"],
            binding.invocation_id
        );
        assert!(!diagnostic.context_json.contains("launch_json"));
        sqlx::query("UPDATE file_broker_launches SET state='closed',shutdown_sha256=?")
            .bind("e".repeat(64))
            .execute(&mut *store.inner.writer.lock().await)
            .await
            .unwrap();
        assert!(
            !store
                .execution_has_unresolved_file_broker(&binding.execution_token)
                .await
                .unwrap()
        );
        assert!(
            store
                .file_broker_recovery_diagnostic()
                .await
                .unwrap()
                .is_none()
        );
    }
}
