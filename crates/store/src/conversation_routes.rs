//! Durable room routing; acknowledgement is information receipt, not completion.
use crate::{Store, StoreError};
use serde::{Deserialize, Serialize};
use sqlx::Connection;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, sqlx::FromRow)]
#[serde(deny_unknown_fields)]
pub struct ConversationRecipient {
    pub agent_id: String,
    pub kind: String,
    pub reason: String,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub struct ConversationDelivery {
    pub message_id: String,
    pub agent_id: String,
    pub kind: String,
    pub reason: String,
}

impl Store {
    /// Stores a complete, immutable route and all inbox entries atomically.
    /// # Errors
    /// Rejects unknown source messages, invalid recipients and changed replay.
    pub async fn route_conversation_message(
        &self,
        message_id: &str,
        recipients: &[ConversationRecipient],
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let mut sorted = recipients.to_vec();
        sorted.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        if sorted.len() > 32
            || sorted
                .windows(2)
                .any(|pair| pair[0].agent_id == pair[1].agent_id)
            || sorted.iter().any(|r| {
                !super::conversations::valid_id(&r.agent_id)
                    || !matches!(r.kind.as_str(), "assignment" | "information")
                    || r.reason.is_empty()
                    || r.reason.len() > 256
            })
        {
            return Err(StoreError::Integrity(
                "invalid conversation recipients".into(),
            ));
        }
        let encoded = serde_json::to_string(&sorted)
            .map_err(|_| StoreError::Integrity("recipient encoding failed".into()))?;
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("INSERT INTO conversation_routes(message_id,recipients_json,created_at_ms) VALUES (?,?,?) ON CONFLICT(message_id) DO NOTHING")
            .bind(message_id).bind(&encoded).bind(now_ms).execute(&mut *tx).await?;
        let saved: String = sqlx::query_scalar(
            "SELECT recipients_json FROM conversation_routes WHERE message_id=?",
        )
        .bind(message_id)
        .fetch_one(&mut *tx)
        .await?;
        if saved != encoded {
            return Err(StoreError::IdempotencyCollision);
        }
        for recipient in sorted {
            sqlx::query("INSERT INTO conversation_deliveries(message_id,agent_id,kind,reason) VALUES (?,?,?,?) ON CONFLICT(message_id,agent_id) DO NOTHING")
                .bind(message_id).bind(recipient.agent_id).bind(recipient.kind).bind(recipient.reason)
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Lists one room member's pending deliveries in source-message order.
    /// # Errors
    /// Returns database errors.
    pub async fn pending_conversation_deliveries(
        &self,
        conversation_id: &str,
        agent_id: &str,
    ) -> Result<Vec<ConversationDelivery>, StoreError> {
        Ok(sqlx::query_as("SELECT d.message_id,d.agent_id,d.kind,d.reason FROM conversation_deliveries d JOIN conversation_messages m ON m.id=d.message_id WHERE m.conversation_id=? AND d.agent_id=? AND d.acknowledged_at_ms IS NULL ORDER BY m.sequence LIMIT 100")
            .bind(conversation_id).bind(agent_id).fetch_all(&self.inner.readers).await?)
    }

    /// Records receipt without launching work or changing task authority.
    /// Internal worker operation; callers must bind the agent to their identity.
    /// # Errors
    /// Rejects missing deliveries and returns database errors.
    pub async fn acknowledge_conversation_delivery(
        &self,
        message_id: &str,
        agent_id: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let mut writer = self.inner.writer.lock().await;
        let changed = sqlx::query("UPDATE conversation_deliveries SET acknowledged_at_ms=COALESCE(acknowledged_at_ms,?) WHERE message_id=? AND agent_id=?")
            .bind(now_ms).bind(message_id).bind(agent_id).execute(&mut *writer).await?.rows_affected();
        if changed != 1 {
            return Err(StoreError::Integrity(
                "conversation delivery missing".into(),
            ));
        }
        Ok(())
    }
}
