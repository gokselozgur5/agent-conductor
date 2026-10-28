-- Preserve immutable intake plans while selecting a successor for retained work.
CREATE TABLE workflow_assignments (
    task_id TEXT PRIMARY KEY REFERENCES workflow_dispatch(task_id),
    worker_id TEXT NOT NULL REFERENCES agents(id),
    pin_json TEXT NOT NULL CHECK(json_valid(pin_json) AND length(pin_json) <= 4096),
    source_attempt_id TEXT NOT NULL,
    expected_version INTEGER NOT NULL CHECK(expected_version >= 0),
    updated_at_ms INTEGER NOT NULL
) STRICT;
