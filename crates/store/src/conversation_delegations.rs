//! Atomic authorization of native delegation proposals. This never queues work.
use crate::{Store, StoreError};
use serde::Deserialize;
use sqlx::{Connection, SqliteConnection};
use std::collections::{BTreeMap, BTreeSet};

/// An immutable assignment authorized for one native recipient, not task acceptance.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, sqlx::FromRow)]
pub struct ConversationAssignment {
    pub source_reply_id: String,
    pub conversation_id: String,
    pub root_message_id: String,
    pub parent_message_id: String,
    pub author_agent_id: String,
    pub agent_id: String,
    pub instruction: String,
    pub depth: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Decision {
    schema: String,
    assignments: Vec<Proposal>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Proposal {
    agent_id: String,
    instruction: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Room {
    coordinator: String,
    members: Vec<Member>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    agent_id: String,
    reports_to: Option<String>,
    observes: BTreeSet<String>,
}
#[derive(sqlx::FromRow)]
struct NativeSource {
    conversation_id: String,
    parent_message_id: String,
    author_agent_id: String,
    body: String,
    configuration: String,
}
#[derive(PartialEq, Eq, sqlx::FromRow)]
struct Batch {
    conversation_id: String,
    root_message_id: String,
    parent_message_id: String,
    author_agent_id: String,
    depth: i64,
    decision_json: String,
    room_configuration_json: String,
}

impl Store {
    /// Re-reads native provenance, the room and the complete decision while owning
    /// the write transaction. Immutable replay returns the same authorization.
    /// No caller-supplied proposal, author or root identity is accepted.
    /// # Errors
    /// Rejects missing/forged provenance, invalid hierarchy or proposal, changed
    /// replay, depth above four and more than 32 assignments per operator request.
    pub async fn authorize_conversation_delegation(
        &self,
        reply_id: &str,
        now_ms: i64,
    ) -> Result<Vec<ConversationAssignment>, StoreError> {
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        let source = native_source(&mut tx, reply_id).await?;
        let assignments = authorize_in(&mut tx, reply_id, source, now_ms).await?;
        tx.commit().await?;
        Ok(assignments)
    }

    /// Reads a bounded immutable authorization batch, without granting execution.
    /// # Errors
    /// Returns database errors.
    pub async fn conversation_assignments(
        &self,
        reply_id: &str,
    ) -> Result<Vec<ConversationAssignment>, StoreError> {
        let mut connection = self.inner.readers.acquire().await?;
        read_assignments(&mut connection, reply_id).await
    }
}

async fn native_source(
    connection: &mut SqliteConnection,
    reply_id: &str,
) -> Result<NativeSource, StoreError> {
    sqlx::query_as("SELECT m.conversation_id,t.message_id AS parent_message_id,t.agent_id AS author_agent_id,m.body,r.configuration_json AS configuration FROM conversation_turns t JOIN conversation_messages m ON m.id=t.reply_message_id JOIN conversation_rooms r ON r.conversation_id=m.conversation_id WHERE m.id=? AND t.state='replied' AND m.sender='agent' AND m.agent_id=t.agent_id AND m.conversation_id=t.conversation_id")
        .bind(reply_id).fetch_optional(connection).await?
        .ok_or_else(|| invalid("settled native room reply required"))
}

async fn authorize_in(
    tx: &mut SqliteConnection,
    reply_id: &str,
    source: NativeSource,
    now_ms: i64,
) -> Result<Vec<ConversationAssignment>, StoreError> {
    let decision = validate_decision(&source)?;
    let (root_message_id, depth) = ancestry(tx, &source).await?;
    let expected = Batch {
        conversation_id: source.conversation_id,
        root_message_id,
        parent_message_id: source.parent_message_id,
        author_agent_id: source.author_agent_id,
        depth,
        decision_json: source.body,
        room_configuration_json: source.configuration,
    };
    let saved: Option<Batch> = sqlx::query_as("SELECT conversation_id,root_message_id,parent_message_id,author_agent_id,depth,decision_json,room_configuration_json FROM conversation_delegation_batches WHERE source_reply_id=?")
        .bind(reply_id).fetch_optional(&mut *tx).await?;
    let assignments = expected_assignments(reply_id, &expected, decision);
    if let Some(saved) = saved {
        if saved != expected || read_assignments(tx, reply_id).await? != assignments {
            return Err(StoreError::IdempotencyCollision);
        }
    } else {
        insert_batch(tx, reply_id, &expected, &assignments, now_ms).await?;
    }
    Ok(assignments)
}

fn validate_decision(source: &NativeSource) -> Result<Decision, StoreError> {
    if source.body.len() > 32_768 || source.configuration.len() > 32_768 {
        return Err(invalid("delegation input exceeds bound"));
    }
    let decision: Decision = serde_json::from_str(&source.body)
        .map_err(|_| invalid("strict delegation decision required"))?;
    if decision.schema != "conductor.delegation.v1"
        || decision.assignments.is_empty()
        || decision.assignments.len() > 8
    {
        return Err(invalid("unsupported or oversized delegation decision"));
    }
    let room = validate_room(&source.configuration)?;
    if !room.contains_key(&source.author_agent_id) {
        return Err(invalid("delegating author is not a room member"));
    }
    let mut targets = BTreeSet::new();
    for assignment in &decision.assignments {
        if assignment.instruction.trim().is_empty()
            || assignment.instruction.len() > 4_096
            || !targets.insert(&assignment.agent_id)
            || room
                .get(&assignment.agent_id)
                .and_then(|m| m.reports_to.as_deref())
                != Some(source.author_agent_id.as_str())
        {
            return Err(invalid("invalid or unauthorized delegation recipient"));
        }
    }
    Ok(decision)
}

fn validate_room(encoded: &str) -> Result<BTreeMap<String, Member>, StoreError> {
    let room: Room =
        serde_json::from_str(encoded).map_err(|_| invalid("invalid stored room topology"))?;
    if room.members.is_empty() || room.members.len() > 32 {
        return Err(invalid("invalid stored room membership"));
    }
    let mut members = BTreeMap::new();
    for member in room.members {
        if !super::conversations::valid_id(&member.agent_id)
            || members.insert(member.agent_id.clone(), member).is_some()
        {
            return Err(invalid("invalid stored room member"));
        }
    }
    if !members.contains_key(&room.coordinator) {
        return Err(invalid("stored room coordinator missing"));
    }
    for member in members.values() {
        for reference in member.reports_to.iter().chain(&member.observes) {
            if reference == &member.agent_id || !members.contains_key(reference) {
                return Err(invalid("invalid stored room relationship"));
            }
        }
        let mut seen = BTreeSet::new();
        let mut cursor = Some(&member.agent_id);
        while let Some(id) = cursor {
            if !seen.insert(id) {
                return Err(invalid("stored room command cycle"));
            }
            cursor = members.get(id).and_then(|m| m.reports_to.as_ref());
        }
    }
    Ok(members)
}

async fn ancestry(
    connection: &mut SqliteConnection,
    source: &NativeSource,
) -> Result<(String, i64), StoreError> {
    let parent: Option<(String, String)> =
        sqlx::query_as("SELECT conversation_id,sender FROM conversation_messages WHERE id=?")
            .bind(&source.parent_message_id)
            .fetch_optional(&mut *connection)
            .await?;
    let (conversation, sender) = parent.ok_or_else(|| invalid("delegation parent missing"))?;
    if conversation != source.conversation_id {
        return Err(invalid("delegation parent belongs to another room"));
    }
    let (root, depth) = if sender == "operator" {
        (source.parent_message_id.clone(), 1)
    } else {
        let inherited: Option<(String, String, i64)> = sqlx::query_as("SELECT b.root_message_id,b.conversation_id,b.depth FROM conversation_assignments a JOIN conversation_delegation_batches b ON b.source_reply_id=a.source_reply_id WHERE a.source_reply_id=? AND a.agent_id=?")
            .bind(&source.parent_message_id).bind(&source.author_agent_id)
            .fetch_optional(&mut *connection).await?;
        let (root, room, depth) =
            inherited.ok_or_else(|| invalid("authorized parent assignment required"))?;
        if room != source.conversation_id || !(1..4).contains(&depth) {
            return Err(invalid("delegation ancestry or depth refused"));
        }
        (root, depth + 1)
    };
    let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM conversation_messages WHERE id=? AND conversation_id=? AND sender='operator' AND agent_id IS NULL)")
        .bind(&root).bind(&source.conversation_id).fetch_one(connection).await?;
    if !valid {
        return Err(invalid("original operator request required in this room"));
    }
    Ok((root, depth))
}

fn expected_assignments(
    reply_id: &str,
    batch: &Batch,
    decision: Decision,
) -> Vec<ConversationAssignment> {
    let mut assignments: Vec<_> = decision
        .assignments
        .into_iter()
        .map(|proposal| ConversationAssignment {
            source_reply_id: reply_id.into(),
            conversation_id: batch.conversation_id.clone(),
            root_message_id: batch.root_message_id.clone(),
            parent_message_id: batch.parent_message_id.clone(),
            author_agent_id: batch.author_agent_id.clone(),
            agent_id: proposal.agent_id,
            instruction: proposal.instruction,
            depth: batch.depth,
        })
        .collect();
    assignments.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
    assignments
}

async fn read_assignments(
    connection: &mut SqliteConnection,
    reply_id: &str,
) -> Result<Vec<ConversationAssignment>, StoreError> {
    Ok(sqlx::query_as("SELECT a.source_reply_id,b.conversation_id,b.root_message_id,b.parent_message_id,b.author_agent_id,a.agent_id,a.instruction,b.depth FROM conversation_assignments a JOIN conversation_delegation_batches b ON b.source_reply_id=a.source_reply_id WHERE a.source_reply_id=? ORDER BY a.agent_id LIMIT 8")
        .bind(reply_id).fetch_all(connection).await?)
}

async fn insert_batch(
    connection: &mut SqliteConnection,
    reply_id: &str,
    batch: &Batch,
    assignments: &[ConversationAssignment],
    now_ms: i64,
) -> Result<(), StoreError> {
    let existing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM conversation_assignments a JOIN conversation_delegation_batches b ON b.source_reply_id=a.source_reply_id WHERE b.root_message_id=?")
        .bind(&batch.root_message_id).fetch_one(&mut *connection).await?;
    let additional =
        i64::try_from(assignments.len()).map_err(|_| invalid("delegation count invalid"))?;
    if existing > 32 - additional {
        return Err(invalid("operator request delegation budget exhausted"));
    }
    sqlx::query("INSERT INTO conversation_delegation_batches(source_reply_id,conversation_id,root_message_id,parent_message_id,author_agent_id,depth,decision_json,room_configuration_json,created_at_ms) VALUES (?,?,?,?,?,?,?,?,?)")
        .bind(reply_id).bind(&batch.conversation_id).bind(&batch.root_message_id)
        .bind(&batch.parent_message_id).bind(&batch.author_agent_id).bind(batch.depth)
        .bind(&batch.decision_json).bind(&batch.room_configuration_json).bind(now_ms)
        .execute(&mut *connection).await?;
    for assignment in assignments {
        sqlx::query("INSERT INTO conversation_assignments(source_reply_id,agent_id,instruction) VALUES (?,?,?)")
            .bind(reply_id).bind(&assignment.agent_id).bind(&assignment.instruction)
            .execute(&mut *connection).await?;
    }
    Ok(())
}

fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}

#[cfg(test)]
#[path = "conversation_delegations_tests.rs"]
mod tests;
