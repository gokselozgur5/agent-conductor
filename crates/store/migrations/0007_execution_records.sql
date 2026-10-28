CREATE TABLE executions (
    effect_token TEXT PRIMARY KEY NOT NULL REFERENCES effect_intents(token),
    task_id TEXT NOT NULL REFERENCES tasks(id),
    attempt_id TEXT NOT NULL UNIQUE REFERENCES attempts(id),
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch > 0),
    runner_id TEXT NOT NULL,
    packet_json TEXT NOT NULL CHECK (json_valid(packet_json)),
    packet_sha256 TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    state TEXT NOT NULL CHECK (state IN ('prepared', 'started', 'exited', 'fenced', 'blocked')),
    wrapper_identity TEXT,
    checkpoint_json TEXT CHECK (checkpoint_json IS NULL OR json_valid(checkpoint_json)),
    exit_code INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE execution_receipts (
    effect_token TEXT NOT NULL REFERENCES executions(effect_token),
    command_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    outcome_json TEXT NOT NULL CHECK (json_valid(outcome_json)),
    PRIMARY KEY(effect_token, command_id)
) STRICT;
