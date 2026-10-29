-- Authenticated account transport is distinct from parent-observed process data.
ALTER TABLE executions ADD COLUMN account_channel INTEGER NOT NULL DEFAULT 0
CHECK(account_channel IN (0,1));
ALTER TABLE executions ADD COLUMN account_peer_json TEXT
CHECK(account_peer_json IS NULL OR
    (json_valid(account_peer_json) AND length(account_peer_json)<=1024 AND process_json IS NULL AND account_channel=1));

CREATE TRIGGER account_execution_refuse_local_exit
BEFORE UPDATE OF state ON executions
WHEN NEW.account_channel=1 AND NEW.state='exited'
BEGIN SELECT RAISE(ABORT, 'account channel is not local process exit evidence'); END;

CREATE TRIGGER execution_transport_is_immutable
BEFORE UPDATE OF account_channel ON executions
WHEN NEW.account_channel!=OLD.account_channel
BEGIN SELECT RAISE(ABORT, 'execution transport is immutable'); END;
