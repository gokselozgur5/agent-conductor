use crate::{Store, StoreError};

impl Store {
    /// Queues a stored message with an immutable trusted agent profile. The
    /// message must be an operator request or a native reply whose delegation
    /// batch authorizes exactly this recipient; agent prose alone is refused.
    /// # Errors
    /// Rejects unknown/unauthorized messages, invalid JSON and changed replay.
    pub async fn queue_conversation_job(
        &self,
        message_id: &str,
        profile_json: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let profile: serde_json::Value = serde_json::from_str(profile_json)
            .map_err(|_| StoreError::Integrity("invalid conversation agent profile".into()))?;
        let agent_id = profile
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|id| super::conversations::valid_id(id))
            .ok_or_else(|| StoreError::Integrity("conversation profile agent missing".into()))?;
        if profile_json.len() > 32_768 {
            return Err(StoreError::Integrity(
                "invalid conversation agent profile".into(),
            ));
        }
        let mut writer = self.inner.writer.lock().await;
        sqlx::query("INSERT INTO conversation_jobs(message_id,agent_id,agent_profile_json,state,created_at_ms) SELECT id,?,?,'queued',? FROM conversation_messages WHERE id=? AND (sender='operator' OR EXISTS(SELECT 1 FROM conversation_assignments a WHERE a.source_reply_id=conversation_messages.id AND a.agent_id=?)) ON CONFLICT(message_id,agent_id) DO NOTHING")
            .bind(agent_id).bind(profile_json).bind(now_ms).bind(message_id).bind(agent_id).execute(&mut *writer).await?;
        let saved: Option<String> = sqlx::query_scalar(
            "SELECT agent_profile_json FROM conversation_jobs WHERE message_id=? AND agent_id=?",
        )
        .bind(message_id)
        .bind(agent_id)
        .fetch_optional(&mut *writer)
        .await?;
        match saved {
            Some(value) if value == profile_json => Ok(()),
            Some(_) => Err(StoreError::IdempotencyCollision),
            None => Err(StoreError::Integrity(
                "operator message or authorized assignment required".into(),
            )),
        }
    }

    /// Selects unreserved work; reservation remains authoritative before launch.
    /// # Errors
    /// Returns database errors.
    pub async fn next_conversation_job(&self) -> Result<Option<(String, String)>, StoreError> {
        Ok(sqlx::query_as("SELECT j.message_id,j.agent_profile_json FROM conversation_jobs j JOIN conversation_messages m ON m.id=j.message_id WHERE j.state='queued' AND NOT EXISTS (SELECT 1 FROM conversation_turns t WHERE t.message_id=j.message_id AND t.agent_id=j.agent_id) AND NOT EXISTS (SELECT 1 FROM conversation_turns t WHERE t.conversation_id=m.conversation_id AND t.state IN ('launch_intent','unknown')) ORDER BY m.sequence,j.agent_id LIMIT 1")
            .fetch_optional(&self.inner.readers).await?)
    }

    /// Stops automatic admission after a refused or uncertain turn.
    /// # Errors
    /// Returns database errors.
    pub async fn block_conversation_job(
        &self,
        message_id: &str,
        profile_json: &str,
    ) -> Result<(), StoreError> {
        let mut writer = self.inner.writer.lock().await;
        sqlx::query("UPDATE conversation_jobs SET state='blocked' WHERE message_id=? AND agent_profile_json=?")
            .bind(message_id)
            .bind(profile_json)
            .execute(&mut *writer)
            .await?;
        Ok(())
    }
}
