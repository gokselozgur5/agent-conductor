//! Durable conversation context; no message changes task or process authority.
use crate::{Store, StoreError};
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct ConversationMessage {
    pub sequence: i64,
    pub id: String,
    pub conversation_id: String,
    pub sender: String,
    pub agent_id: Option<String>,
    pub body: String,
    pub created_at_ms: i64,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct ConversationHeader {
    pub id: String,
    pub project_path: String,
    pub created_at_ms: i64,
}

impl Store {
    /// Lists a bounded page of durable conversations.
    /// # Errors
    /// Returns database errors.
    pub async fn list_conversations(
        &self,
        after: &str,
    ) -> Result<Vec<ConversationHeader>, StoreError> {
        Ok(sqlx::query_as("SELECT id,project_path,created_at_ms FROM conversations WHERE id>? ORDER BY id LIMIT 20")
            .bind(after).fetch_all(&self.inner.readers).await?)
    }

    /// Observes durable message execution state without claiming process liveness.
    /// # Errors
    /// Returns database errors.
    pub async fn conversation_message_status(&self, id: &str) -> Result<String, StoreError> {
        Ok(sqlx::query_scalar("SELECT CASE WHEN m.sender='agent' THEN 'replied' ELSE COALESCE(t.state,j.state,'saved') END FROM conversation_messages m LEFT JOIN conversation_jobs j ON j.message_id=m.id LEFT JOIN conversation_turns t ON t.message_id=m.id WHERE m.id=?")
            .bind(id).fetch_one(&self.inner.readers).await?)
    }

    /// Checks whether another message page exists.
    /// # Errors
    /// Returns database errors.
    pub async fn conversation_has_more(&self, id: &str, after: i64) -> Result<bool, StoreError> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM conversation_messages WHERE conversation_id=? AND sequence>?)")
            .bind(id).bind(after).fetch_one(&self.inner.readers).await?)
    }
    /// Looks up an immutable message for native turn admission.
    /// # Errors
    /// Returns database errors.
    pub async fn conversation_message(
        &self,
        id: &str,
    ) -> Result<Option<ConversationMessage>, StoreError> {
        Ok(sqlx::query_as("SELECT sequence,id,conversation_id,sender,agent_id,body,created_at_ms FROM conversation_messages WHERE id=?")
            .bind(id).fetch_optional(&self.inner.readers).await?)
    }

    /// Returns the conversation's immutable project binding.
    /// # Errors
    /// Returns database errors or a missing conversation error.
    pub async fn conversation_project(&self, id: &str) -> Result<String, StoreError> {
        Ok(
            sqlx::query_scalar("SELECT project_path FROM conversations WHERE id=?")
                .bind(id)
                .fetch_one(&self.inner.readers)
                .await?,
        )
    }

    /// Creates a conversation, or verifies an identical project binding on replay.
    /// # Errors
    /// Rejects invalid identifiers, relative project paths and conflicting replay.
    pub async fn create_conversation(
        &self,
        id: &str,
        project_path: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        if !valid_id(id)
            || project_path.len() > 4096
            || !std::path::Path::new(project_path).is_absolute()
        {
            return Err(StoreError::Integrity("invalid conversation binding".into()));
        }
        let mut writer = self.inner.writer.lock().await;
        sqlx::query("INSERT INTO conversations(id,project_path,created_at_ms) VALUES (?,?,?) ON CONFLICT(id) DO NOTHING")
            .bind(id).bind(project_path).bind(now_ms).execute(&mut *writer).await?;
        let saved: String = sqlx::query_scalar("SELECT project_path FROM conversations WHERE id=?")
            .bind(id)
            .fetch_one(&mut *writer)
            .await?;
        if saved != project_path {
            return Err(StoreError::IdempotencyCollision);
        }
        Ok(())
    }

    /// Appends immutable text. The caller must authenticate the sender; this API
    /// neither invokes a model nor interprets text as a service command.
    /// Sequence and timestamp are assigned here; replay returns the original row.
    /// # Errors
    /// Rejects malformed messages, unknown conversations and conflicting replay.
    pub async fn append_conversation_message(
        &self,
        message: &ConversationMessage,
        now_ms: i64,
    ) -> Result<ConversationMessage, StoreError> {
        let sender_valid = match message.sender.as_str() {
            "operator" => message.agent_id.is_none(),
            "agent" => message.agent_id.as_deref().is_some_and(valid_id),
            _ => false,
        };
        if !valid_id(&message.id)
            || !valid_id(&message.conversation_id)
            || !sender_valid
            || message.body.trim().is_empty()
            || message.body.len() > 65_536
        {
            return Err(StoreError::Integrity("invalid conversation message".into()));
        }
        let mut writer = self.inner.writer.lock().await;
        sqlx::query("INSERT INTO conversation_messages(id,conversation_id,sender,agent_id,body,created_at_ms) VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING")
            .bind(&message.id).bind(&message.conversation_id).bind(&message.sender)
            .bind(&message.agent_id).bind(&message.body).bind(now_ms)
            .execute(&mut *writer).await?;
        let saved: ConversationMessage = sqlx::query_as("SELECT sequence,id,conversation_id,sender,agent_id,body,created_at_ms FROM conversation_messages WHERE id=?")
            .bind(&message.id).fetch_one(&mut *writer).await?;
        if saved.conversation_id != message.conversation_id
            || saved.sender != message.sender
            || saved.agent_id != message.agent_id
            || saved.body != message.body
        {
            return Err(StoreError::IdempotencyCollision);
        }
        Ok(saved)
    }

    /// Returns up to 100 messages after an exclusive, server-assigned cursor.
    /// # Errors
    /// Rejects invalid identifiers/cursors and returns database errors.
    pub async fn conversation_messages(
        &self,
        conversation_id: &str,
        after: i64,
    ) -> Result<Vec<ConversationMessage>, StoreError> {
        if !valid_id(conversation_id) || after < 0 {
            return Err(StoreError::Integrity("invalid conversation cursor".into()));
        }
        Ok(sqlx::query_as("SELECT sequence,id,conversation_id,sender,agent_id,body,created_at_ms FROM conversation_messages WHERE conversation_id=? AND sequence>? ORDER BY sequence LIMIT 100")
            .bind(conversation_id).bind(after).fetch_all(&self.inner.readers).await?)
    }
}

pub(super) fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b':'))
}
