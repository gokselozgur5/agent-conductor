PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    path TEXT NOT NULL UNIQUE,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY,
    adapter TEXT NOT NULL,
    capabilities_json TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'idle'
        CHECK (status IN ('idle', 'busy', 'offline')),
    last_heartbeat REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    project_id TEXT REFERENCES projects(id),
    title TEXT NOT NULL,
    state TEXT NOT NULL
        CHECK (state IN ('queued', 'claimed', 'running', 'verifying', 'completed', 'blocked', 'cancelled')),
    owner_agent_id TEXT REFERENCES agents(id),
    lease_expires_at REAL,
    attempt INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 0,
    required_checks_json TEXT NOT NULL DEFAULT '[]',
    evidence_json TEXT NOT NULL DEFAULT '{}',
    checkpoint_json TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    completed_at REAL
);

CREATE TABLE IF NOT EXISTS verifications (
    task_id TEXT NOT NULL REFERENCES tasks(id),
    verifier_agent_id TEXT NOT NULL REFERENCES agents(id),
    verdict TEXT NOT NULL CHECK (verdict IN ('accepted', 'rejected')),
    checks_json TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL,
    PRIMARY KEY (task_id, verifier_agent_id, created_at)
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    correlation_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 0),
    source_agent_id TEXT REFERENCES agents(id),
    topic TEXT NOT NULL,
    path TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL REFERENCES agents(id),
    topic_pattern TEXT NOT NULL,
    path_pattern TEXT,
    UNIQUE (agent_id, topic_pattern, path_pattern)
);

CREATE TABLE IF NOT EXISTS deliveries (
    event_id TEXT NOT NULL REFERENCES events(id),
    agent_id TEXT NOT NULL REFERENCES agents(id),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'acknowledged')),
    created_at REAL NOT NULL,
    acknowledged_at REAL,
    PRIMARY KEY (event_id, agent_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state);
CREATE INDEX IF NOT EXISTS idx_tasks_project_state ON tasks(project_id, state);
CREATE INDEX IF NOT EXISTS idx_tasks_lease ON tasks(lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_agent_status ON deliveries(agent_id, status);
