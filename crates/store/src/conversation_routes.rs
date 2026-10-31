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

/// Why one recipient received a message and what became of it, for display.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct ConversationRouting {
    pub agent_id: String,
    pub kind: String,
    pub reason: String,
    pub state: String,
}

/// Execution state of a recipient's turn on a message, or its job if no turn.
const RECIPIENT_STATE: &str = "COALESCE((SELECT t.state FROM conversation_turns t WHERE t.message_id=?1 AND t.agent_id=r.agent_id),(SELECT j.state FROM conversation_jobs j WHERE j.message_id=?1 AND j.agent_id=r.agent_id),'pending')";

impl Store {
    /// Lists every recipient of a message: routed deliveries and delegated
    /// assignments, with the reason and the current delivery or execution state.
    /// # Errors
    /// Returns database errors.
    pub async fn conversation_message_routing(
        &self,
        message_id: &str,
    ) -> Result<Vec<ConversationRouting>, StoreError> {
        let query = format!(
            "SELECT r.agent_id,r.kind,r.reason,CASE WHEN r.kind='information' THEN CASE WHEN r.acknowledged_at_ms IS NULL THEN 'pending' ELSE 'received' END ELSE {RECIPIENT_STATE} END AS state FROM (SELECT agent_id,kind,reason,acknowledged_at_ms FROM conversation_deliveries WHERE message_id=?1 UNION ALL SELECT agent_id,'delegated','delegated_by_supervisor',NULL FROM conversation_assignments WHERE source_reply_id=?1) r ORDER BY r.kind,r.agent_id LIMIT 64"
        );
        Ok(sqlx::query_as(&query)
            .bind(message_id)
            .fetch_all(&self.inner.readers)
            .await?)
    }

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

    /// Settles a native reply that no room route can describe, so it stops
    /// heading the unrouted queue. It gets an empty route (nobody is informed)
    /// and a `rejected` examination carrying the reason; a reply with a
    /// recorded route or examination keeps what it has.
    /// # Errors
    /// Rejects non-agent or unknown messages and returns database errors.
    pub async fn reject_unroutable_conversation_reply(
        &self,
        reply_id: &str,
        reason: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        let agent_reply: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM conversation_messages WHERE id=? AND sender='agent'")
                .bind(reply_id)
                .fetch_optional(&mut *tx)
                .await?;
        if agent_reply.is_none() {
            return Err(StoreError::Integrity(
                "recorded agent reply required".into(),
            ));
        }
        sqlx::query("INSERT INTO conversation_routes(message_id,recipients_json,created_at_ms) VALUES (?,'[]',?) ON CONFLICT(message_id) DO NOTHING")
            .bind(reply_id).bind(now_ms).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO conversation_reply_examinations(reply_message_id,outcome,detail,created_at_ms) VALUES (?,'rejected',?,?) ON CONFLICT(reply_message_id) DO NOTHING")
            .bind(reply_id).bind(reason).bind(now_ms).execute(&mut *tx).await?;
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
