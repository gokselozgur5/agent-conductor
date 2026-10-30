use crate::{Store, StoreError};

impl Store {
    /// Queues a stored operator message with an immutable trusted agent profile.
    /// # Errors
    /// Rejects unknown/non-operator messages, invalid JSON and changed replay.
    pub async fn queue_conversation_job(
        &self,
        message_id: &str,
        profile_json: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        if profile_json.len() > 32_768
            || !serde_json::from_str::<serde_json::Value>(profile_json).is_ok_and(|v| v.is_object())
        {
            return Err(StoreError::Integrity(
                "invalid conversation agent profile".into(),
            ));
        }
        let mut writer = self.inner.writer.lock().await;
        sqlx::query("INSERT INTO conversation_jobs(message_id,agent_profile_json,state,created_at_ms) SELECT id,?,'queued',? FROM conversation_messages WHERE id=? AND sender='operator' ON CONFLICT(message_id) DO NOTHING")
            .bind(profile_json).bind(now_ms).bind(message_id).execute(&mut *writer).await?;
        let saved: Option<String> = sqlx::query_scalar(
            "SELECT agent_profile_json FROM conversation_jobs WHERE message_id=?",
        )
        .bind(message_id)
        .fetch_optional(&mut *writer)
        .await?;
        match saved {
            Some(value) if value == profile_json => Ok(()),
            Some(_) => Err(StoreError::IdempotencyCollision),
            None => Err(StoreError::Integrity("operator message required".into())),
        }
    }

    /// Selects unreserved work; reservation remains authoritative before launch.
    /// # Errors
    /// Returns database errors.
    pub async fn next_conversation_job(&self) -> Result<Option<(String, String)>, StoreError> {
        Ok(sqlx::query_as("SELECT j.message_id,j.agent_profile_json FROM conversation_jobs j JOIN conversation_messages m ON m.id=j.message_id WHERE j.state='queued' AND NOT EXISTS (SELECT 1 FROM conversation_turns t WHERE t.message_id=j.message_id) AND NOT EXISTS (SELECT 1 FROM conversation_turns t WHERE t.conversation_id=m.conversation_id AND t.state IN ('launch_intent','unknown')) ORDER BY m.sequence LIMIT 1")
            .fetch_optional(&self.inner.readers).await?)
    }

    /// Stops automatic admission after a refused or uncertain turn.
    /// # Errors
    /// Returns database errors.
    pub async fn block_conversation_job(&self, message_id: &str) -> Result<(), StoreError> {
        let mut writer = self.inner.writer.lock().await;
        sqlx::query("UPDATE conversation_jobs SET state='blocked' WHERE message_id=?")
            .bind(message_id)
            .execute(&mut *writer)
            .await?;
        Ok(())
    }
}
