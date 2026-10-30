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

impl Store {
    /// Creates a conversation, or verifies an identical project binding on replay.
    /// # Errors
    /// Rejects invalid identifiers, relative project paths and conflicting replay.
    pub async fn create_conversation(
        &self,
        id: &str,
        project_path: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        if !valid_id(id) || !std::path::Path::new(project_path).is_absolute() {
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
