-- Non-secret native broker launch binding, committed before process creation.
CREATE TABLE file_broker_launches (
    invocation_id TEXT PRIMARY KEY NOT NULL,
    execution_token TEXT NOT NULL UNIQUE REFERENCES executions(effect_token),
    binding_json TEXT NOT NULL CHECK(json_valid(binding_json) AND length(binding_json)<=4096),
    launch_json TEXT NOT NULL CHECK(json_valid(launch_json) AND length(launch_json)<=16384),
    state TEXT NOT NULL CHECK(state IN ('open','closed')),
    created_at_ms INTEGER NOT NULL,
    shutdown_sha256 TEXT CHECK(shutdown_sha256 IS NULL OR length(shutdown_sha256)=64),
    CHECK((state='closed')=(shutdown_sha256 IS NOT NULL))
) STRICT;

DROP VIEW unresolved_file_broker_tasks;
CREATE VIEW unresolved_file_broker_tasks AS
SELECT DISTINCT e.task_id FROM file_brokers b
JOIN executions e ON e.effect_token=b.execution_token
WHERE b.state='open' OR EXISTS (
    SELECT 1 FROM file_operations o WHERE o.invocation_id=b.invocation_id AND o.completed_at_ms IS NULL)
UNION
SELECT e.task_id FROM file_broker_launches b
JOIN executions e ON e.effect_token=b.execution_token WHERE b.state='open';

CREATE TRIGGER file_broker_launch_refuse_fence
BEFORE UPDATE OF state ON executions
WHEN NEW.state='fenced' AND EXISTS (
    SELECT 1 FROM file_broker_launches b WHERE b.execution_token=NEW.effect_token AND b.state='open')
BEGIN SELECT RAISE(ABORT, 'file broker launch ownership unresolved'); END;
