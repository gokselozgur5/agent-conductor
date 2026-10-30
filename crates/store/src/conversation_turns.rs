//! A durable one-shot launch reservation. Replayed intent never grants a launch.
use crate::{Store, StoreError};
use sqlx::Connection;

#[derive(sqlx::FromRow)]
struct ReplyBinding {
    conversation_id: String,
    agent_id: String,
    state: String,
    reply_message_id: Option<String>,
    output_sha256: Option<String>,
}

pub struct ConversationTurnIntent<'a> {
    pub message_id: &'a str,
    pub agent_id: &'a str,
    pub launch_id: &'a str,
    pub profile_sha256: &'a str,
    pub prompt_sha256: &'a str,
}

impl Store {
    /// Retains an uncertain execution without granting another launch.
    /// # Errors
    /// Returns database errors.
    pub async fn mark_conversation_turn_unknown(&self, launch_id: &str) -> Result<(), StoreError> {
        let mut writer = self.inner.writer.lock().await;
        sqlx::query("UPDATE conversation_turns SET state='unknown' WHERE launch_id=? AND state='launch_intent'")
            .bind(launch_id).execute(&mut *writer).await?;
        Ok(())
    }

    /// Publishes a verified native reply and settles its launch in one transaction.
    /// This is an internal runtime operation, never an operator RPC capability.
    /// # Errors
    /// Rejects missing launches, conflicting replies and invalid text/digests.
    pub async fn publish_conversation_reply(
        &self,
        launch_id: &str,
        reply_id: &str,
        body: &str,
        output_sha256: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        if !super::conversations::valid_id(reply_id)
            || body.trim().is_empty()
            || body.len() > 65_536
            || output_sha256.len() != 64
            || !output_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(StoreError::Integrity("invalid conversation reply".into()));
        }
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        let turn: Option<ReplyBinding> = sqlx::query_as("SELECT conversation_id,agent_id,state,reply_message_id,output_sha256 FROM conversation_turns WHERE launch_id=?")
            .bind(launch_id).fetch_optional(&mut *tx).await?;
        let Some(ReplyBinding {
            conversation_id: conversation,
            agent_id: agent,
            state,
            reply_message_id: saved_reply,
            output_sha256: saved_digest,
        }) = turn
        else {
            return Err(StoreError::Integrity("conversation launch missing".into()));
        };
        if state == "replied" {
            let saved_body: Option<String> =
                sqlx::query_scalar("SELECT body FROM conversation_messages WHERE id=?")
                    .bind(reply_id)
                    .fetch_optional(&mut *tx)
                    .await?;
            if saved_reply.as_deref() != Some(reply_id)
                || saved_digest.as_deref() != Some(output_sha256)
                || saved_body.as_deref() != Some(body)
            {
                return Err(StoreError::IdempotencyCollision);
            }
        } else if state == "launch_intent" || state == "unknown" {
            sqlx::query("INSERT INTO conversation_messages(id,conversation_id,sender,agent_id,body,created_at_ms) VALUES (?,?,'agent',?,?,?)")
                .bind(reply_id).bind(&conversation).bind(&agent).bind(body).bind(now_ms).execute(&mut *tx).await?;
            sqlx::query("UPDATE conversation_turns SET state='replied',reply_message_id=?,output_sha256=? WHERE launch_id=?")
                .bind(reply_id).bind(output_sha256).bind(launch_id).execute(&mut *tx).await?;
        } else {
            return Err(StoreError::Integrity(
                "conversation turn cannot accept a reply".into(),
            ));
        }
        tx.commit().await?;
        Ok(())
    }

    /// Reserves an operator message for exactly one native launch attempt.
    /// Only a fresh `true` result permits the caller to initiate execution.
    /// A crash after reservation leaves uncertainty; replay must not launch again.
    /// # Errors
    /// Rejects changed bindings, non-operator messages and overlapping turns.
    pub async fn reserve_conversation_turn(
        &self,
        intent: &ConversationTurnIntent<'_>,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        if [intent.message_id, intent.agent_id, intent.launch_id]
            .iter()
            .any(|v| !super::conversations::valid_id(v))
            || [intent.profile_sha256, intent.prompt_sha256]
                .iter()
                .any(|v| {
                    v.len() != 64
                        || !v
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
        {
            return Err(StoreError::Integrity(
                "invalid conversation turn binding".into(),
            ));
        }
        let mut writer = self.inner.writer.lock().await;
        let inserted = sqlx::query("INSERT INTO conversation_turns(message_id,conversation_id,agent_id,launch_id,profile_sha256,prompt_sha256,state,created_at_ms) SELECT id,conversation_id,?,?,?,?,'launch_intent',? FROM conversation_messages WHERE id=? AND sender='operator' ON CONFLICT(message_id) DO NOTHING")
            .bind(intent.agent_id).bind(intent.launch_id).bind(intent.profile_sha256)
            .bind(intent.prompt_sha256).bind(now_ms).bind(intent.message_id)
            .execute(&mut *writer).await?.rows_affected();
        let saved: Option<(String, String, String, String)> = sqlx::query_as("SELECT agent_id,launch_id,profile_sha256,prompt_sha256 FROM conversation_turns WHERE message_id=?")
            .bind(intent.message_id).fetch_optional(&mut *writer).await?;
        let expected = (
            intent.agent_id.into(),
            intent.launch_id.into(),
            intent.profile_sha256.into(),
            intent.prompt_sha256.into(),
        );
        match saved {
            Some(value) if value == expected => Ok(inserted == 1),
            Some(_) => Err(StoreError::IdempotencyCollision),
            None => Err(StoreError::Integrity(
                "operator message required for conversation turn".into(),
            )),
        }
    }
}
