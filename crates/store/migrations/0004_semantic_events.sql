CREATE TABLE event_subscriptions (
    id TEXT PRIMARY KEY NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    topic_filter TEXT NOT NULL,
    project_id TEXT REFERENCES projects(id) ON DELETE RESTRICT,
    path_prefix TEXT,
    echo_source INTEGER NOT NULL DEFAULT 0 CHECK (echo_source IN (0, 1)),
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX active_event_subscriptions
ON event_subscriptions(topic_filter, project_id, agent_id)
WHERE active = 1;

CREATE TABLE semantic_events (
    id TEXT PRIMARY KEY NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    idempotency_key TEXT NOT NULL UNIQUE,
    source_agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    source_session_id TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id TEXT REFERENCES semantic_events(id) ON DELETE RESTRICT,
    generation INTEGER NOT NULL CHECK (generation >= 0),
    topic TEXT NOT NULL,
    project_id TEXT REFERENCES projects(id) ON DELETE RESTRICT,
    task_id TEXT REFERENCES tasks(id) ON DELETE RESTRICT,
    path_scope TEXT,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL CHECK (expires_at_ms >= created_at_ms),
    max_retries INTEGER NOT NULL CHECK (max_retries > 0)
) STRICT;

CREATE INDEX semantic_events_expiry ON semantic_events(expires_at_ms);

CREATE TABLE event_deliveries (
    id TEXT PRIMARY KEY NOT NULL,
    event_id TEXT NOT NULL REFERENCES semantic_events(id) ON DELETE RESTRICT,
    subscription_id TEXT NOT NULL REFERENCES event_subscriptions(id) ON DELETE RESTRICT,
    target_agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'leased', 'acked', 'stale', 'dead')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at_ms INTEGER NOT NULL,
    lease_expires_at_ms INTEGER,
    acked_at_ms INTEGER,
    stale_reason TEXT,
    UNIQUE(event_id, subscription_id)
) STRICT;

CREATE INDEX claimable_event_deliveries
ON event_deliveries(target_agent_id, status, available_at_ms);

CREATE TABLE event_delivery_audit (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id TEXT NOT NULL REFERENCES event_deliveries(id) ON DELETE RESTRICT,
    status TEXT NOT NULL CHECK (status IN ('stale', 'dead')),
    reason TEXT NOT NULL,
    occurred_at_ms INTEGER NOT NULL,
    UNIQUE(delivery_id, status)
) STRICT;

CREATE TABLE consumer_state (
    namespace TEXT NOT NULL,
    state_key TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 0),
    value_json TEXT NOT NULL CHECK (json_valid(value_json)),
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY(namespace, state_key)
) STRICT, WITHOUT ROWID;
