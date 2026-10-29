-- Historical verification is not process-settlement evidence. Preserve missing
-- ownership separately rather than inventing an invocation, PID or receipt.
CREATE TABLE legacy_auxiliary_ownership (
    task_id TEXT PRIMARY KEY NOT NULL REFERENCES tasks(id),
    reason TEXT NOT NULL CHECK (reason IN ('verification_without_invocation', 'unbound_pending_invocation')),
    recorded_boot_generation INTEGER NOT NULL CHECK (recorded_boot_generation >= 0)
) STRICT;

INSERT INTO legacy_auxiliary_ownership
SELECT t.id, 'verification_without_invocation', r.boot_generation
FROM tasks t JOIN runtime_metadata r ON r.singleton=1
WHERE t.state != 'completed'
AND (t.state='verifying' OR json_type(t.aggregate_json,'$.verification')='object'
     OR EXISTS (SELECT 1 FROM task_transitions x WHERE x.task_id=t.id AND x.new_state='verifying'))
AND NOT EXISTS (SELECT 1 FROM auxiliary_invocations a WHERE a.task_id=t.id);

INSERT OR IGNORE INTO legacy_auxiliary_ownership
SELECT a.task_id, 'unbound_pending_invocation', r.boot_generation
FROM auxiliary_invocations a JOIN runtime_metadata r ON r.singleton=1
WHERE a.state='pending' AND a.launch_binding_json IS NULL;

CREATE TRIGGER legacy_auxiliary_refuse_dispatch
BEFORE UPDATE OF status ON workflow_dispatch
WHEN NEW.status IN ('queued','running') AND EXISTS (SELECT 1 FROM legacy_auxiliary_ownership WHERE task_id=NEW.task_id)
BEGIN SELECT RAISE(ABORT, 'legacy auxiliary ownership unresolved'); END;

CREATE TRIGGER legacy_auxiliary_refuse_claim
BEFORE UPDATE OF state ON tasks
WHEN NEW.state='claimed' AND EXISTS (SELECT 1 FROM legacy_auxiliary_ownership WHERE task_id=NEW.id)
BEGIN SELECT RAISE(ABORT, 'legacy auxiliary ownership unresolved'); END;

CREATE TRIGGER legacy_auxiliary_refuse_execution
BEFORE INSERT ON executions
WHEN EXISTS (SELECT 1 FROM legacy_auxiliary_ownership WHERE task_id=NEW.task_id)
BEGIN SELECT RAISE(ABORT, 'legacy auxiliary ownership unresolved'); END;

CREATE TRIGGER legacy_auxiliary_refuse_invocation
BEFORE INSERT ON auxiliary_invocations
WHEN EXISTS (SELECT 1 FROM legacy_auxiliary_ownership WHERE task_id=NEW.task_id)
BEGIN SELECT RAISE(ABORT, 'legacy auxiliary ownership unresolved'); END;

CREATE TRIGGER legacy_auxiliary_refuse_new_dispatch
BEFORE INSERT ON workflow_dispatch
WHEN EXISTS (SELECT 1 FROM legacy_auxiliary_ownership WHERE task_id=NEW.task_id)
BEGIN SELECT RAISE(ABORT, 'legacy auxiliary ownership unresolved'); END;
