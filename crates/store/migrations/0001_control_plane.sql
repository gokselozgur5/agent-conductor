CREATE TABLE projects (
    id TEXT PRIMARY KEY NOT NULL,
    canonical_path TEXT NOT NULL UNIQUE,
    filesystem_identity TEXT NOT NULL UNIQUE,
    git_common_dir_identity TEXT,
    created_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE workspaces (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    canonical_path TEXT NOT NULL UNIQUE,
    filesystem_identity TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('main', 'worktree', 'directory')),
    created_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE agents (
    id TEXT PRIMARY KEY NOT NULL,
    adapter_kind TEXT NOT NULL,
    protocol_fingerprint TEXT NOT NULL,
    capability_hash TEXT NOT NULL,
    access_mode TEXT NOT NULL CHECK (access_mode IN ('read_only', 'read_write')),
    created_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE tasks (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    parent_task_id TEXT REFERENCES tasks(id) ON DELETE RESTRICT,
    objective TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'backlog', 'ready', 'claimed', 'running', 'paused',
        'verifying', 'blocked', 'completed', 'cancelled'
    )),
    priority INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    authority_json TEXT NOT NULL CHECK (json_valid(authority_json)),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE attempts (
    id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    worker_id TEXT NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    workspace_id TEXT REFERENCES workspaces(id) ON DELETE RESTRICT,
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch > 0),
    start_tree_identity TEXT,
    instruction_hashes_json TEXT NOT NULL CHECK (json_valid(instruction_hashes_json)),
    required_checks_json TEXT NOT NULL CHECK (json_valid(required_checks_json)),
    capability_hash TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    ended_at_ms INTEGER
) STRICT;

CREATE UNIQUE INDEX one_open_attempt_per_task
ON attempts(task_id) WHERE ended_at_ms IS NULL;

CREATE UNIQUE INDEX lease_epoch_per_task
ON attempts(task_id, lease_epoch);

CREATE TABLE leases (
    attempt_id TEXT PRIMARY KEY NOT NULL REFERENCES attempts(id) ON DELETE RESTRICT,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    epoch INTEGER NOT NULL CHECK (epoch > 0),
    status TEXT NOT NULL CHECK (status IN ('active', 'recovery_pending', 'expired', 'released')),
    boot_generation INTEGER NOT NULL CHECK (boot_generation >= 0),
    remaining_ms INTEGER NOT NULL CHECK (remaining_ms >= 0),
    grace_ms INTEGER NOT NULL CHECK (grace_ms >= 0),
    display_expires_at_ms INTEGER NOT NULL,
    last_heartbeat_at_ms INTEGER NOT NULL,
    UNIQUE (task_id, epoch)
) STRICT;

CREATE UNIQUE INDEX one_live_lease_per_task
ON leases(task_id) WHERE status IN ('active', 'recovery_pending');

CREATE TABLE workspace_claims (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE RESTRICT,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE RESTRICT,
    mode TEXT NOT NULL CHECK (mode IN ('read', 'write')),
    released_at_ms INTEGER
) STRICT;

CREATE UNIQUE INDEX one_write_owner_per_workspace
ON workspace_claims(workspace_id)
WHERE mode = 'write' AND released_at_ms IS NULL;

CREATE TABLE task_transitions (
    id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    actor_id TEXT NOT NULL,
    prior_state TEXT NOT NULL,
    new_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    occurred_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    task_id TEXT REFERENCES tasks(id) ON DELETE RESTRICT,
    actor_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    occurred_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX audit_events_by_task_sequence
ON audit_events(task_id, sequence);

CREATE TABLE effect_intents (
    token TEXT PRIMARY KEY NOT NULL,
    task_id TEXT REFERENCES tasks(id) ON DELETE RESTRICT,
    attempt_id TEXT REFERENCES attempts(id) ON DELETE RESTRICT,
    lease_epoch INTEGER,
    correlation_id TEXT NOT NULL,
    effect_kind TEXT NOT NULL,
    request_json TEXT NOT NULL CHECK (json_valid(request_json)),
    status TEXT NOT NULL CHECK (status IN ('pending', 'claimed', 'observed', 'done', 'abandoned')),
    runner_id TEXT,
    claim_deadline_ms INTEGER,
    observation_json TEXT CHECK (observation_json IS NULL OR json_valid(observation_json)),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX pending_effect_intents
ON effect_intents(status, created_at_ms);

CREATE TABLE command_idempotency (
    actor_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    outcome_json TEXT NOT NULL CHECK (json_valid(outcome_json)),
    committed_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    PRIMARY KEY (actor_id, idempotency_key)
) STRICT, WITHOUT ROWID;

CREATE INDEX command_idempotency_expiry
ON command_idempotency(expires_at_ms);
