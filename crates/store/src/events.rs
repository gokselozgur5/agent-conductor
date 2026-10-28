use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection};
use thiserror::Error;
use uuid::Uuid;

use crate::Store;

pub const EVENT_SCHEMA_VERSION: u16 = 1;

// Aliases s (subscription) and e (event) are shared across publication, leasing
// and wake admission. Only a trailing .* is a topic-family operator; prefixes
// are literal and case-sensitive, independent of SQLite LIKE settings.
pub(crate) const SUBSCRIPTION_MATCH_SQL: &str = "s.active = 1 AND (s.topic_filter = e.topic OR (substr(s.topic_filter, -2) = '.*' AND instr(e.topic, substr(s.topic_filter, 1, length(s.topic_filter) - 1)) = 1)) AND (s.project_id IS NULL OR s.project_id = e.project_id) AND (s.path_prefix IS NULL OR instr(e.path_scope, s.path_prefix) = 1) AND (s.echo_source = 1 OR s.agent_id <> e.source_agent_id)";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventLimits {
    pub max_generation: u32,
    pub max_fanout: usize,
    pub max_retries: u32,
    pub max_age_ms: i64,
    pub max_payload_bytes: usize,
    pub delivery_lease_ms: i64,
}

impl Default for EventLimits {
    fn default() -> Self {
        Self {
            max_generation: 8,
            max_fanout: 64,
            max_retries: 5,
            max_age_ms: 24 * 60 * 60 * 1_000,
            max_payload_bytes: 64 * 1_024,
            delivery_lease_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSourceIdentity<'a> {
    pub source_agent_id: &'a str,
    pub source_session_id: &'a str,
    pub normalized_payload: &'a str,
}

impl EventSourceIdentity<'_> {
    #[must_use]
    pub fn idempotency_key(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"agent-conductor:event-idempotency:v1\0");
        digest.update(self.source_agent_id.as_bytes());
        digest.update(b"\0");
        digest.update(self.source_session_id.as_bytes());
        digest.update(b"\0");
        digest.update(self.normalized_payload.as_bytes());
        format!("event:v1:{:x}", digest.finalize())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventSubscription {
    pub schema_version: u16,
    pub id: String,
    pub agent_id: String,
    pub topic_filter: String,
    pub project_id: Option<String>,
    pub path_prefix: Option<String>,
    #[serde(default)]
    pub echo_source: bool,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticEvent {
    pub schema_version: u16,
    pub id: String,
    pub idempotency_key: String,
    pub source_agent_id: String,
    pub source_session_id: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub generation: u32,
    pub topic: String,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub attempt_id: Option<String>,
    pub lease_epoch: Option<u64>,
    pub path_scope: Option<String>,
    pub payload_json: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventDelivery {
    pub schema_version: u16,
    pub delivery_id: String,
    pub event: SemanticEvent,
    pub attempts: u32,
    pub claim_token: String,
    pub lease_expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishResult {
    Published { event_id: String, fanout: usize },
    Duplicate { event_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerStateEffect {
    pub namespace: String,
    pub key: String,
    pub expected_version: Option<u64>,
    pub value_json: String,
}

#[derive(Debug, Error)]
pub enum EventStoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("unsupported event schema version: {0}")]
    UnsupportedSchema(u16),
    #[error("event payload is not valid JSON")]
    InvalidPayload,
    #[error("event payload exceeds the configured byte limit")]
    PayloadTooLarge,
    #[error("event generation exceeds the configured limit")]
    GenerationLimit,
    #[error("event generation does not follow its causal parent")]
    InvalidCausalGeneration,
    #[error("event age is invalid or exceeds the configured limit")]
    AgeLimit,
    #[error("event fan-out exceeds the configured limit")]
    FanoutLimit,
    #[error("event retry policy exceeds the configured limit")]
    RetryLimit,
    #[error("event idempotency key was reused for another event")]
    IdempotencyCollision,
    #[error("delivery not found or not leased by this consumer")]
    DeliveryNotLeased,
    #[error("consumer state version conflict")]
    ConsumerStateConflict,
    #[error("numeric event field cannot be represented")]
    NumericRange,
}

/// Bound request for atomic hook-event publication and baseline advancement.
pub struct RepositoryObservation<'a> {
    pub id: &'a str,
    pub config_json: &'a str,
    pub expected_version: Option<i64>,
    pub snapshot_json: &'a str,
    pub now_ms: i64,
}

impl Store {
    /// Adds or replaces a durable semantic subscription.
    pub async fn upsert_event_subscription(
        &self,
        subscription: &EventSubscription,
    ) -> Result<(), EventStoreError> {
        validate_schema(subscription.schema_version)?;
        let mut writer = self.inner.writer.lock().await;
        sqlx::query(
            "INSERT INTO event_subscriptions (id, schema_version, agent_id, topic_filter, project_id, path_prefix, echo_source, active, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?) ON CONFLICT(id) DO UPDATE SET schema_version = excluded.schema_version, agent_id = excluded.agent_id, topic_filter = excluded.topic_filter, project_id = excluded.project_id, path_prefix = excluded.path_prefix, echo_source = excluded.echo_source, active = 1",
        )
        .bind(&subscription.id)
        .bind(i64::from(subscription.schema_version))
        .bind(&subscription.agent_id)
        .bind(&subscription.topic_filter)
        .bind(&subscription.project_id)
        .bind(&subscription.path_prefix)
        .bind(subscription.echo_source)
        .bind(subscription.created_at_ms)
        .execute(&mut *writer)
        .await?;
        self.signal_dispatch();
        Ok(())
    }

    /// Publishes an event and materializes every matching delivery atomically.
    pub async fn publish_event(
        &self,
        event: &SemanticEvent,
        limits: &EventLimits,
    ) -> Result<PublishResult, EventStoreError> {
        validate_event(event, limits)?;
        let mut connection = self.inner.writer.lock().await;
        let mut writer = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = publish_in_transaction(&mut writer, event, limits).await;
        let committed = finish_event_transaction(writer, result).await?;
        if matches!(committed, PublishResult::Published { fanout, .. } if fanout > 0) {
            self.signal_dispatch();
        }
        Ok(committed)
    }

    /// Reads the last atomically published repository observation.
    pub async fn repository_observation(
        &self,
        id: &str,
    ) -> Result<Option<(String, i64, String)>, EventStoreError> {
        Ok(sqlx::query_as(
            "SELECT config_json,version,snapshot_json FROM repository_observations WHERE id=?",
        )
        .bind(id)
        .fetch_optional(&self.inner.readers)
        .await?)
    }

    /// Commits a bounded hook batch and its new baseline in the same transaction.
    /// A failed event or stale baseline rolls back every delivery and observation.
    /// Config identity is immutable; an empty initial batch establishes baseline.
    pub async fn publish_repository_observation(
        &self,
        observation: &RepositoryObservation<'_>,
        events: &[SemanticEvent],
        limits: &EventLimits,
    ) -> Result<(), EventStoreError> {
        if observation.id.is_empty()
            || observation.id.len() > 128
            || observation.config_json.len() > 16_384
            || observation.snapshot_json.len() > 8 * 1024 * 1024
            || events.len() > 256
            || observation.expected_version.is_none() && !events.is_empty()
        {
            return Err(EventStoreError::InvalidPayload);
        }
        for json in [observation.config_json, observation.snapshot_json] {
            serde_json::from_str::<serde_json::Value>(json)
                .map_err(|_| EventStoreError::InvalidPayload)?;
        }
        for event in events {
            validate_event(event, limits)?;
        }
        let version = match observation.expected_version {
            Some(v) if v >= 0 => v.checked_add(1).ok_or(EventStoreError::NumericRange)?,
            None => 0,
            _ => return Err(EventStoreError::NumericRange),
        };
        let mut connection = self.inner.writer.lock().await;
        let mut writer = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = async {
            let stored: Option<(String,i64)> = sqlx::query_as("SELECT config_json,version FROM repository_observations WHERE id=?")
                .bind(observation.id).fetch_optional(&mut *writer).await?;
            match (stored, observation.expected_version) {
                (None,None) => {},
                (Some((config,current)),Some(expected)) if config == observation.config_json && current == expected => {},
                _ => return Err(EventStoreError::ConsumerStateConflict),
            }
            for event in events { publish_in_transaction(&mut writer,event,limits).await?; }
            sqlx::query("INSERT INTO repository_observations(id,config_json,version,snapshot_json,updated_at_ms) VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET version=excluded.version,snapshot_json=excluded.snapshot_json,updated_at_ms=excluded.updated_at_ms")
                .bind(observation.id).bind(observation.config_json).bind(version)
                .bind(observation.snapshot_json).bind(observation.now_ms).execute(&mut *writer).await?;
            Ok(())
        }.await;
        finish_event_transaction(writer, result).await?;
        if !events.is_empty() {
            self.signal_dispatch();
        }
        Ok(())
    }

    /// Claims one delivery. An expired lease is deliberately replayable.
    pub async fn claim_event_delivery(
        &self,
        agent_id: &str,
        now_ms: i64,
        limits: &EventLimits,
    ) -> Result<Option<EventDelivery>, EventStoreError> {
        let lease_expires_at_ms = now_ms
            .checked_add(limits.delivery_lease_ms)
            .ok_or(EventStoreError::NumericRange)?;
        let mut connection = self.inner.writer.lock().await;
        let mut writer = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = async {
            expire_stale_in_transaction(&mut writer, now_ms).await?;
            let row = sqlx::query(&format!(
                "SELECT d.id, d.attempts, e.id, e.schema_version, e.idempotency_key, e.source_agent_id, e.source_session_id, e.correlation_id, e.causation_id, e.generation, e.topic, e.project_id, e.task_id, e.attempt_id, e.lease_epoch, e.path_scope, e.payload_json, e.created_at_ms, e.max_retries FROM event_deliveries d JOIN semantic_events e ON e.id = d.event_id JOIN event_subscriptions s ON s.id=d.subscription_id AND s.agent_id=d.target_agent_id WHERE {SUBSCRIPTION_MATCH_SQL} AND d.target_agent_id = ? AND ((d.status = 'pending' AND d.available_at_ms <= ?) OR (d.status = 'leased' AND d.lease_expires_at_ms <= ?)) AND e.expires_at_ms > ? AND d.attempts < e.max_retries ORDER BY e.created_at_ms, d.id LIMIT 1",
            ))
            .bind(agent_id)
            .bind(now_ms)
            .bind(now_ms)
            .bind(now_ms)
            .fetch_optional(&mut *writer)
            .await?;
            let Some(row) = row else { return Ok(None); };
            let delivery_id: String = row.try_get(0)?;
            let claim_token = Uuid::now_v7().to_string();
            sqlx::query("UPDATE event_deliveries SET status = 'leased', attempts = attempts + 1, claim_token = ?, lease_expires_at_ms = ? WHERE id = ?")
                .bind(&claim_token)
                .bind(lease_expires_at_ms)
                .bind(&delivery_id)
                .execute(&mut *writer)
                .await?;
            let prior_attempts: i64 = row.try_get(1)?;
            let attempts = u32::try_from(prior_attempts + 1).map_err(|_| EventStoreError::NumericRange)?;
            Ok(Some(delivery_from_row(delivery_id, attempts, claim_token, lease_expires_at_ms, &row)?))
        }
        .await;
        finish_event_transaction(writer, result).await
    }

    /// Applies Conductor-owned consumer state and acknowledges a delivery in one commit.
    pub async fn acknowledge_event_with_state(
        &self,
        delivery_id: &str,
        consumer_agent_id: &str,
        claim_token: &str,
        effect: &ConsumerStateEffect,
        now_ms: i64,
    ) -> Result<(), EventStoreError> {
        if serde_json::from_str::<serde_json::Value>(&effect.value_json).is_err() {
            return Err(EventStoreError::InvalidPayload);
        }
        let mut connection = self.inner.writer.lock().await;
        let mut writer = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = async {
            expire_stale_in_transaction(&mut writer, now_ms).await?;
            let current_claim: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM event_deliveries d JOIN semantic_events e ON e.id = d.event_id WHERE d.id = ? AND d.target_agent_id = ? AND d.status = 'leased' AND d.claim_token = ? AND d.lease_expires_at_ms > ? AND e.expires_at_ms > ?",
            )
            .bind(delivery_id)
            .bind(consumer_agent_id)
            .bind(claim_token)
            .bind(now_ms)
            .bind(now_ms)
            .fetch_optional(&mut *writer)
            .await?;
            if current_claim.is_none() {
                return Err(EventStoreError::DeliveryNotLeased);
            }
            let current = sqlx::query_scalar::<_, i64>(
                "SELECT version FROM consumer_state WHERE namespace = ? AND state_key = ?",
            )
            .bind(&effect.namespace)
            .bind(&effect.key)
            .fetch_optional(&mut *writer)
            .await?;
            match (current, effect.expected_version) {
                (None, None) => {
                    sqlx::query("INSERT INTO consumer_state (namespace, state_key, version, value_json, updated_at_ms) VALUES (?, ?, 0, ?, ?)")
                        .bind(&effect.namespace)
                        .bind(&effect.key)
                        .bind(&effect.value_json)
                        .bind(now_ms)
                        .execute(&mut *writer)
                        .await?;
                }
                (Some(version), Some(expected)) if u64::try_from(version).ok() == Some(expected) => {
                    sqlx::query("UPDATE consumer_state SET version = version + 1, value_json = ?, updated_at_ms = ? WHERE namespace = ? AND state_key = ? AND version = ?")
                        .bind(&effect.value_json)
                        .bind(now_ms)
                        .bind(&effect.namespace)
                        .bind(&effect.key)
                        .bind(version)
                        .execute(&mut *writer)
                        .await?;
                }
                _ => return Err(EventStoreError::ConsumerStateConflict),
            }
            let changed = sqlx::query("UPDATE event_deliveries SET status = 'acked', acked_at_ms = ?, claim_token = NULL, lease_expires_at_ms = NULL WHERE id = ? AND target_agent_id = ? AND status = 'leased' AND claim_token = ?")
                .bind(now_ms)
                .bind(delivery_id)
                .bind(consumer_agent_id)
                .bind(claim_token)
                .execute(&mut *writer)
                .await?;
            if changed.rows_affected() != 1 {
                return Err(EventStoreError::DeliveryNotLeased);
            }
            Ok(())
        }
        .await;
        finish_event_transaction(writer, result).await
    }

    pub async fn consumer_state(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<(u64, String)>, EventStoreError> {
        let row = sqlx::query_as::<_, (i64, String)>(
            "SELECT version, value_json FROM consumer_state WHERE namespace = ? AND state_key = ?",
        )
        .bind(namespace)
        .bind(key)
        .fetch_optional(&self.inner.readers)
        .await?;
        row.map(|(version, value)| {
            Ok((
                u64::try_from(version).map_err(|_| EventStoreError::NumericRange)?,
                value,
            ))
        })
        .transpose()
    }

    pub async fn event_delivery_count(&self, event_id: &str) -> Result<i64, EventStoreError> {
        Ok(
            sqlx::query_scalar("SELECT COUNT(*) FROM event_deliveries WHERE event_id = ?")
                .bind(event_id)
                .fetch_one(&self.inner.readers)
                .await?,
        )
    }
}

fn validate_schema(version: u16) -> Result<(), EventStoreError> {
    if version == EVENT_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(EventStoreError::UnsupportedSchema(version))
    }
}

fn validate_event(event: &SemanticEvent, limits: &EventLimits) -> Result<(), EventStoreError> {
    validate_schema(event.schema_version)?;
    if event.generation > limits.max_generation {
        return Err(EventStoreError::GenerationLimit);
    }
    if event.payload_json.len() > limits.max_payload_bytes {
        return Err(EventStoreError::PayloadTooLarge);
    }
    if serde_json::from_str::<serde_json::Value>(&event.payload_json).is_err() {
        return Err(EventStoreError::InvalidPayload);
    }
    let age = limits.max_age_ms;
    if age <= 0 || limits.max_retries == 0 || limits.delivery_lease_ms <= 0 {
        return Err(EventStoreError::AgeLimit);
    }
    Ok(())
}

async fn publish_in_transaction(
    connection: &mut SqliteConnection,
    event: &SemanticEvent,
    limits: &EventLimits,
) -> Result<PublishResult, EventStoreError> {
    if let Some(row) = sqlx::query(
        "SELECT id, schema_version, source_agent_id, source_session_id, correlation_id, causation_id, generation, topic, project_id, task_id, attempt_id, lease_epoch, path_scope, payload_json FROM semantic_events WHERE idempotency_key = ?",
    )
    .bind(&event.idempotency_key)
    .fetch_optional(&mut *connection)
    .await?
    {
        let stored_id: String = row.try_get(0)?;
        if event_matches_row(event, &row)? {
            return Ok(PublishResult::Duplicate { event_id: stored_id });
        }
        return Err(EventStoreError::IdempotencyCollision);
    }
    if sqlx::query_scalar::<_, String>("SELECT idempotency_key FROM semantic_events WHERE id = ?")
        .bind(&event.id)
        .fetch_optional(&mut *connection)
        .await?
        .is_some()
    {
        return Err(EventStoreError::IdempotencyCollision);
    }
    if let Some(causation_id) = &event.causation_id {
        let parent_generation: Option<i64> =
            sqlx::query_scalar("SELECT generation FROM semantic_events WHERE id = ?")
                .bind(causation_id)
                .fetch_optional(&mut *connection)
                .await?;
        let Some(parent_generation) = parent_generation else {
            return Err(EventStoreError::InvalidCausalGeneration);
        };
        if i64::from(event.generation) != parent_generation + 1 {
            return Err(EventStoreError::InvalidCausalGeneration);
        }
    } else if event.generation != 0 {
        return Err(EventStoreError::InvalidCausalGeneration);
    }
    let expires_at_ms = event
        .created_at_ms
        .checked_add(limits.max_age_ms)
        .ok_or(EventStoreError::NumericRange)?;
    let lease_epoch = event
        .lease_epoch
        .map(i64::try_from)
        .transpose()
        .map_err(|_| EventStoreError::NumericRange)?;
    let subscriptions = sqlx::query_as::<_, (String, String)>(&format!(
        "SELECT s.id, s.agent_id FROM event_subscriptions s CROSS JOIN (SELECT ? AS topic, ? AS project_id, ? AS path_scope, ? AS source_agent_id) e WHERE {SUBSCRIPTION_MATCH_SQL} ORDER BY s.id",
    ))
    .bind(&event.topic)
    .bind(&event.project_id)
    .bind(&event.path_scope)
    .bind(&event.source_agent_id)
    .fetch_all(&mut *connection)
    .await?;
    if subscriptions.len() > limits.max_fanout {
        return Err(EventStoreError::FanoutLimit);
    }
    sqlx::query("INSERT INTO semantic_events (id, schema_version, idempotency_key, source_agent_id, source_session_id, correlation_id, causation_id, generation, topic, project_id, task_id, attempt_id, lease_epoch, path_scope, payload_json, created_at_ms, expires_at_ms, max_retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(&event.id)
        .bind(i64::from(event.schema_version))
        .bind(&event.idempotency_key)
        .bind(&event.source_agent_id)
        .bind(&event.source_session_id)
        .bind(&event.correlation_id)
        .bind(&event.causation_id)
        .bind(i64::from(event.generation))
        .bind(&event.topic)
        .bind(&event.project_id)
        .bind(&event.task_id)
        .bind(&event.attempt_id)
        .bind(lease_epoch)
        .bind(&event.path_scope)
        .bind(&event.payload_json)
        .bind(event.created_at_ms)
        .bind(expires_at_ms)
        .bind(i64::from(limits.max_retries))
        .execute(&mut *connection)
        .await?;
    for (subscription_id, agent_id) in &subscriptions {
        sqlx::query("INSERT INTO event_deliveries (id, event_id, subscription_id, target_agent_id, status, attempts, available_at_ms) VALUES (?, ?, ?, ?, 'pending', 0, ?)")
            .bind(format!("{}:{}", event.id, subscription_id))
            .bind(&event.id)
            .bind(subscription_id)
            .bind(agent_id)
            .bind(event.created_at_ms)
            .execute(&mut *connection)
            .await?;
    }
    Ok(PublishResult::Published {
        event_id: event.id.clone(),
        fanout: subscriptions.len(),
    })
}

async fn expire_stale_in_transaction(
    connection: &mut SqliteConnection,
    now_ms: i64,
) -> Result<(), EventStoreError> {
    sqlx::query("INSERT OR IGNORE INTO event_delivery_audit (delivery_id, status, reason, occurred_at_ms) SELECT d.id, 'stale', 'maximum queue age exceeded', ? FROM event_deliveries d JOIN semantic_events e ON e.id = d.event_id WHERE d.status IN ('pending', 'leased') AND e.expires_at_ms <= ?")
        .bind(now_ms)
        .bind(now_ms)
        .execute(&mut *connection)
        .await?;
    sqlx::query("UPDATE event_deliveries SET status = 'stale', stale_reason = 'maximum queue age exceeded', claim_token = NULL, lease_expires_at_ms = NULL WHERE status IN ('pending', 'leased') AND event_id IN (SELECT id FROM semantic_events WHERE expires_at_ms <= ?)")
        .bind(now_ms)
        .execute(&mut *connection)
        .await?;
    sqlx::query("INSERT OR IGNORE INTO event_delivery_audit (delivery_id, status, reason, occurred_at_ms) SELECT d.id, 'dead', 'maximum retries exceeded', ? FROM event_deliveries d JOIN semantic_events e ON e.id = d.event_id WHERE d.status = 'leased' AND d.lease_expires_at_ms <= ? AND d.attempts >= e.max_retries")
        .bind(now_ms)
        .bind(now_ms)
        .execute(&mut *connection)
        .await?;
    sqlx::query("UPDATE event_deliveries SET status = 'dead', stale_reason = 'maximum retries exceeded', claim_token = NULL, lease_expires_at_ms = NULL WHERE status = 'leased' AND lease_expires_at_ms <= ? AND attempts >= (SELECT max_retries FROM semantic_events WHERE id = event_deliveries.event_id)")
        .bind(now_ms)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

fn delivery_from_row(
    delivery_id: String,
    attempts: u32,
    claim_token: String,
    lease_expires_at_ms: i64,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<EventDelivery, EventStoreError> {
    let schema_version =
        u16::try_from(row.try_get::<i64, _>(3)?).map_err(|_| EventStoreError::NumericRange)?;
    validate_schema(schema_version)?;
    Ok(EventDelivery {
        schema_version,
        delivery_id,
        attempts,
        claim_token,
        lease_expires_at_ms,
        event: SemanticEvent {
            schema_version,
            id: row.try_get(2)?,
            idempotency_key: row.try_get(4)?,
            source_agent_id: row.try_get(5)?,
            source_session_id: row.try_get(6)?,
            correlation_id: row.try_get(7)?,
            causation_id: row.try_get(8)?,
            generation: u32::try_from(row.try_get::<i64, _>(9)?)
                .map_err(|_| EventStoreError::NumericRange)?,
            topic: row.try_get(10)?,
            project_id: row.try_get(11)?,
            task_id: row.try_get(12)?,
            attempt_id: row.try_get(13)?,
            lease_epoch: row
                .try_get::<Option<i64>, _>(14)?
                .map(u64::try_from)
                .transpose()
                .map_err(|_| EventStoreError::NumericRange)?,
            path_scope: row.try_get(15)?,
            payload_json: row.try_get(16)?,
            created_at_ms: row.try_get(17)?,
        },
    })
}

fn event_matches_row(
    event: &SemanticEvent,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<bool, EventStoreError> {
    let lease_epoch = row
        .try_get::<Option<i64>, _>(11)?
        .map(u64::try_from)
        .transpose()
        .map_err(|_| EventStoreError::NumericRange)?;
    Ok(i64::from(event.schema_version) == row.try_get::<i64, _>(1)?
        && event.source_agent_id == row.try_get::<String, _>(2)?
        && event.source_session_id == row.try_get::<String, _>(3)?
        && event.correlation_id == row.try_get::<String, _>(4)?
        && event.causation_id == row.try_get::<Option<String>, _>(5)?
        && i64::from(event.generation) == row.try_get::<i64, _>(6)?
        && event.topic == row.try_get::<String, _>(7)?
        && event.project_id == row.try_get::<Option<String>, _>(8)?
        && event.task_id == row.try_get::<Option<String>, _>(9)?
        && event.attempt_id == row.try_get::<Option<String>, _>(10)?
        && event.lease_epoch == lease_epoch
        && event.path_scope == row.try_get::<Option<String>, _>(12)?
        && event.payload_json == row.try_get::<String, _>(13)?)
}

async fn finish_event_transaction<T>(
    transaction: sqlx::Transaction<'_, sqlx::Sqlite>,
    result: Result<T, EventStoreError>,
) -> Result<T, EventStoreError> {
    let value = result?;
    transaction.commit().await?;
    Ok(value)
}
