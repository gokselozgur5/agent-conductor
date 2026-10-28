CREATE TABLE workflow_dependencies (
    task_id TEXT NOT NULL REFERENCES workflow_dispatch(task_id),
    predecessor_task_id TEXT NOT NULL REFERENCES workflow_dispatch(task_id),
    accepted_source TEXT,
    PRIMARY KEY (task_id, predecessor_task_id),
    CHECK (task_id <> predecessor_task_id)
) STRICT;
