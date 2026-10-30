use crate::{Store, StoreError};

impl Store {
    /// Saves the validated room topology. Replay cannot change active authority.
    /// # Errors
    /// Rejects oversized/invalid configuration or a changed room binding.
    pub async fn configure_conversation_room(
        &self,
        conversation_id: &str,
        configuration: &str,
    ) -> Result<(), StoreError> {
        if configuration.len() > 32_768
            || serde_json::from_str::<serde_json::Value>(configuration).is_err()
        {
            return Err(StoreError::Integrity("invalid room configuration".into()));
        }
        let mut writer = self.inner.writer.lock().await;
        sqlx::query(
            "INSERT INTO conversation_rooms VALUES (?,?) ON CONFLICT(conversation_id) DO NOTHING",
        )
        .bind(conversation_id)
        .bind(configuration)
        .execute(&mut *writer)
        .await?;
        let saved: String = sqlx::query_scalar(
            "SELECT configuration_json FROM conversation_rooms WHERE conversation_id=?",
        )
        .bind(conversation_id)
        .fetch_one(&mut *writer)
        .await?;
        if saved != configuration {
            return Err(StoreError::IdempotencyCollision);
        }
        Ok(())
    }

    /// Reads operator-owned room configuration.
    /// # Errors
    /// Returns database errors.
    pub async fn conversation_room(
        &self,
        conversation_id: &str,
    ) -> Result<Option<String>, StoreError> {
        Ok(sqlx::query_scalar(
            "SELECT configuration_json FROM conversation_rooms WHERE conversation_id=?",
        )
        .bind(conversation_id)
        .fetch_optional(&self.inner.readers)
        .await?)
    }

    /// Finds a native reply needing automatic information routing, including
    /// replies published before a coordinator restart.
    /// # Errors
    /// Returns database errors.
    pub async fn next_unrouted_room_reply(&self) -> Result<Option<(String, String)>, StoreError> {
        Ok(sqlx::query_as("SELECT m.id,m.conversation_id FROM conversation_messages m JOIN conversation_rooms r ON r.conversation_id=m.conversation_id WHERE m.sender='agent' AND NOT EXISTS(SELECT 1 FROM conversation_routes route WHERE route.message_id=m.id) ORDER BY m.sequence LIMIT 1")
            .fetch_optional(&self.inner.readers).await?)
    }

    /// Finds a durable assignment whose native job has not yet been admitted.
    /// Information-only deliveries never enter this execution queue.
    /// # Errors
    /// Returns database errors.
    pub async fn next_room_assignment(&self) -> Result<Option<(String, String)>, StoreError> {
        Ok(sqlx::query_as("SELECT d.message_id,d.agent_id FROM conversation_deliveries d JOIN conversation_messages m ON m.id=d.message_id WHERE d.kind='assignment' AND NOT EXISTS(SELECT 1 FROM conversation_jobs j WHERE j.message_id=d.message_id AND j.agent_id=d.agent_id) ORDER BY m.sequence,d.agent_id LIMIT 1")
            .fetch_optional(&self.inner.readers).await?)
    }
}
