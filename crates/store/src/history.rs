use crate::{Store, StoreError};

/// Deliberately excludes payload_json and any command or checkpoint content.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct AuditMetadata {
    pub sequence: i64,
    pub kind: String,
    pub actor_id: String,
    pub correlation_id: String,
    pub occurred_at_ms: i64,
}

impl Store {
    /// Returns at most 101 records (100 plus a pagination sentinel).
    /// # Errors
    /// Returns database read errors.
    pub async fn task_history(
        &self,
        task_id: &str,
        before: Option<i64>,
    ) -> Result<Vec<AuditMetadata>, StoreError> {
        Ok(sqlx::query_as("SELECT sequence, kind, actor_id, correlation_id, occurred_at_ms FROM audit_events WHERE task_id = ? AND sequence < ? ORDER BY sequence DESC LIMIT 101")
            .bind(task_id).bind(before.unwrap_or(i64::MAX))
            .fetch_all(&self.inner.readers).await?)
    }
}
