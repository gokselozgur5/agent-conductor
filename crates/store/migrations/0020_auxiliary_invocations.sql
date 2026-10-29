-- A writer fence does not settle checks or reviewers launched afterward.
CREATE TABLE auxiliary_invocations (
    invocation_id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    execution_token TEXT NOT NULL REFERENCES executions(effect_token),
    preparation_json TEXT NOT NULL CHECK (json_valid(preparation_json)),
    boot_generation INTEGER NOT NULL CHECK (boot_generation > 0),
    state TEXT NOT NULL CHECK (state IN ('pending', 'settled')),
    created_at_ms INTEGER NOT NULL,
    settled_at_ms INTEGER
) STRICT;
CREATE UNIQUE INDEX one_pending_auxiliary_per_task
ON auxiliary_invocations(task_id) WHERE state = 'pending';
