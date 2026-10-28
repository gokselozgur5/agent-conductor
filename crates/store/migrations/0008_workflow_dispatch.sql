-- Dispatch receipts do not replace the authoritative task lifecycle.
CREATE TABLE workflow_dispatch (
    task_id TEXT PRIMARY KEY,
    plan_json TEXT NOT NULL CHECK(length(plan_json) <= 131072),
    status TEXT NOT NULL CHECK(status IN ('queued','running','finished','needs_recovery')),
    boot_generation INTEGER,
    outcome_json TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
