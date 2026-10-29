-- An open broker is an unresolved filesystem capability even with no pending row.
CREATE VIEW unresolved_file_broker_tasks AS
SELECT DISTINCT e.task_id FROM file_brokers b
JOIN executions e ON e.effect_token=b.execution_token WHERE b.state='open' OR EXISTS (SELECT 1 FROM file_operations o WHERE o.invocation_id=b.invocation_id AND o.completed_at_ms IS NULL);

CREATE TRIGGER file_broker_refuse_fence
BEFORE UPDATE OF state ON executions
WHEN NEW.state='fenced' AND EXISTS (
    SELECT 1 FROM file_brokers b WHERE execution_token=NEW.effect_token AND (state='open' OR EXISTS (SELECT 1 FROM file_operations o WHERE o.invocation_id=b.invocation_id AND o.completed_at_ms IS NULL)))
BEGIN SELECT RAISE(ABORT, 'file broker ownership unresolved'); END;

CREATE TRIGGER file_broker_refuse_task_progress
BEFORE UPDATE OF state ON tasks
WHEN NEW.state IN ('claimed','verifying','completed') AND NEW.state!=OLD.state
AND EXISTS (SELECT 1 FROM unresolved_file_broker_tasks WHERE task_id=NEW.id)
BEGIN SELECT RAISE(ABORT, 'file broker ownership unresolved'); END;

-- Attempt persistence uses INSERT ... ON CONFLICT for heartbeat/release updates.
-- Preserve the existing identity path while refusing another attempt or epoch.
CREATE TRIGGER file_broker_refuse_attempt
BEFORE INSERT ON attempts
WHEN EXISTS (SELECT 1 FROM unresolved_file_broker_tasks WHERE task_id=NEW.task_id)
AND NOT EXISTS (SELECT 1 FROM attempts WHERE id=NEW.id AND task_id=NEW.task_id
                AND worker_id=NEW.worker_id AND lease_epoch=NEW.lease_epoch)
BEGIN SELECT RAISE(ABORT, 'file broker ownership unresolved'); END;

CREATE TRIGGER file_broker_refuse_execution
BEFORE INSERT ON executions
WHEN EXISTS (SELECT 1 FROM unresolved_file_broker_tasks WHERE task_id=NEW.task_id)
BEGIN SELECT RAISE(ABORT, 'file broker ownership unresolved'); END;

CREATE TRIGGER file_broker_refuse_auxiliary
BEFORE INSERT ON auxiliary_invocations
WHEN EXISTS (SELECT 1 FROM unresolved_file_broker_tasks WHERE task_id=NEW.task_id)
BEGIN SELECT RAISE(ABORT, 'file broker ownership unresolved'); END;

CREATE TRIGGER file_broker_refuse_dispatch
BEFORE UPDATE OF status ON workflow_dispatch
WHEN NEW.status IN ('queued','running','finished') AND NEW.status!=OLD.status
AND EXISTS (SELECT 1 FROM unresolved_file_broker_tasks WHERE task_id=NEW.task_id)
BEGIN SELECT RAISE(ABORT, 'file broker ownership unresolved'); END;

CREATE TRIGGER file_broker_refuse_new_dispatch
BEFORE INSERT ON workflow_dispatch
WHEN EXISTS (SELECT 1 FROM unresolved_file_broker_tasks WHERE task_id=NEW.task_id)
BEGIN SELECT RAISE(ABORT, 'file broker ownership unresolved'); END;
