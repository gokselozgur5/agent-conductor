//! Supervisor continuations: one native turn per settled delegation batch.
use crate::{Store, StoreError};

/// A recipient is settled when its job is blocked, its turn failed, or it replied
/// with something other than an authorized delegation. A recipient that
/// delegated further settles only after its own continuation turn settles.
const SETTLED: &str = "EXISTS(SELECT 1 FROM conversation_jobs j WHERE j.message_id=a.source_reply_id AND j.agent_id=a.agent_id AND j.state='blocked') OR EXISTS(SELECT 1 FROM conversation_turns t WHERE t.message_id=a.source_reply_id AND t.agent_id=a.agent_id AND t.state='failed') OR EXISTS(SELECT 1 FROM conversation_turns t JOIN conversation_reply_examinations e ON e.reply_message_id=t.reply_message_id WHERE t.message_id=a.source_reply_id AND t.agent_id=a.agent_id AND t.state='replied' AND (e.outcome<>'authorized' OR EXISTS(SELECT 1 FROM conversation_turns c WHERE c.message_id=t.reply_message_id AND c.agent_id=a.agent_id AND c.state IN ('replied','failed'))))";

impl Store {
    /// Finds the oldest delegation batch without a continuation whose every
    /// assigned recipient has settled. Unknown outcomes never settle a recipient.
    /// # Errors
    /// Returns database errors.
    pub async fn next_ready_continuation(&self) -> Result<Option<(String, String)>, StoreError> {
        let query = format!(
            "SELECT b.source_reply_id,b.author_agent_id FROM conversation_delegation_batches b JOIN conversation_messages m ON m.id=b.source_reply_id WHERE NOT EXISTS(SELECT 1 FROM conversation_continuations c WHERE c.source_reply_id=b.source_reply_id) AND NOT EXISTS(SELECT 1 FROM conversation_assignments a WHERE a.source_reply_id=b.source_reply_id AND NOT ({SETTLED})) ORDER BY m.sequence LIMIT 1"
        );
        Ok(sqlx::query_as(&query)
            .fetch_optional(&self.inner.readers)
            .await?)
    }

    /// Opens the single continuation for a settled batch, bound to its author.
    /// Replay is idempotent; an unsettled batch or another author is refused.
    /// # Errors
    /// Rejects unsettled or unknown batches and returns database errors.
    pub async fn open_conversation_continuation(
        &self,
        reply_id: &str,
        now_ms: i64,
    ) -> Result<String, StoreError> {
        let mut writer = self.inner.writer.lock().await;
        let insert = format!(
            "INSERT INTO conversation_continuations(source_reply_id,agent_id,created_at_ms) SELECT b.source_reply_id,b.author_agent_id,? FROM conversation_delegation_batches b WHERE b.source_reply_id=? AND NOT EXISTS(SELECT 1 FROM conversation_assignments a WHERE a.source_reply_id=b.source_reply_id AND NOT ({SETTLED})) ON CONFLICT(source_reply_id) DO NOTHING"
        );
        sqlx::query(&insert)
            .bind(now_ms)
            .bind(reply_id)
            .execute(&mut *writer)
            .await?;
        sqlx::query_scalar(
            "SELECT agent_id FROM conversation_continuations WHERE source_reply_id=?",
        )
        .bind(reply_id)
        .fetch_optional(&mut *writer)
        .await?
        .ok_or_else(|| StoreError::Integrity("settled delegation batch required".into()))
    }

    /// Finds an opened continuation whose native job was never admitted, such as
    /// after a crash between opening and admission.
    /// # Errors
    /// Returns database errors.
    pub async fn next_unadmitted_continuation(
        &self,
    ) -> Result<Option<(String, String)>, StoreError> {
        Ok(sqlx::query_as("SELECT c.source_reply_id,c.agent_id FROM conversation_continuations c JOIN conversation_messages m ON m.id=c.source_reply_id WHERE NOT EXISTS(SELECT 1 FROM conversation_jobs j WHERE j.message_id=c.source_reply_id AND j.agent_id=c.agent_id) ORDER BY m.sequence LIMIT 1")
            .fetch_optional(&self.inner.readers)
            .await?)
    }

    /// Whether a continuation admits this agent's turn on this decision message.
    /// # Errors
    /// Returns database errors.
    pub async fn conversation_continuation(
        &self,
        reply_id: &str,
        agent_id: &str,
    ) -> Result<bool, StoreError> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM conversation_continuations WHERE source_reply_id=? AND agent_id=?)")
            .bind(reply_id)
            .bind(agent_id)
            .fetch_one(&self.inner.readers)
            .await?)
    }
}
