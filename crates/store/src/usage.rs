use crate::{Store, StoreError};
impl Store {
    /// Inserts one immutable usage observation; identical replay is a no-op.
    /// This single-statement insert does not change task or lease authority.
    /// # Errors
    /// Rejects malformed/oversized metadata, conflicting replay or database errors.
    pub async fn record_usage(
        &self,
        id: &str,
        task_id: &str,
        attempt_id: &str,
        metadata: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        if metadata.len() > 16_384
            || !serde_json::from_str::<serde_json::Value>(metadata).is_ok_and(|v| v.is_object())
        {
            return Err(StoreError::Integrity("invalid usage metadata".into()));
        }
        let mut writer = self.inner.writer.lock().await;
        sqlx::query("INSERT INTO audit_events (id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES (?,?,'conductor-runtime','vendor.usage_reported',?,?,?) ON CONFLICT(id) DO NOTHING")
            .bind(id).bind(task_id).bind(attempt_id).bind(metadata).bind(now_ms).execute(&mut *writer).await?;
        let saved: (String, String, String, String) = sqlx::query_as(
            "SELECT task_id,kind,correlation_id,payload_json FROM audit_events WHERE id=?",
        )
        .bind(id)
        .fetch_one(&mut *writer)
        .await?;
        if saved
            != (
                task_id.into(),
                "vendor.usage_reported".into(),
                attempt_id.into(),
                metadata.into(),
            )
        {
            return Err(StoreError::IdempotencyCollision);
        }
        Ok(())
    }
}
