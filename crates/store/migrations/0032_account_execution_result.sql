-- Account reports and imported candidates are not local process exit evidence.
ALTER TABLE executions ADD COLUMN account_result_json TEXT
CHECK(account_result_json IS NULL OR
    (json_valid(account_result_json) AND length(account_result_json)<=1024
     AND account_channel=1 AND account_peer_json IS NOT NULL AND process_json IS NULL));

DROP VIEW capture_eligible_executions;
CREATE VIEW capture_eligible_executions AS
SELECT e.* FROM executions e
WHERE (
    (e.account_channel=1 AND e.state='started' AND e.account_peer_json IS NOT NULL
     AND e.process_json IS NULL AND e.wrapper_identity IS NOT NULL AND json_type(e.account_result_json,'$.snapshot_sha256')='text'
     AND length(json_extract(e.account_result_json,'$.snapshot_sha256'))=64
     AND EXISTS (SELECT 1 FROM tasks t WHERE t.id=e.task_id
         AND json_extract(t.aggregate_json,'$.verification.source')=
             'snapshot:' || json_extract(e.account_result_json,'$.snapshot_sha256')))
    OR     e.state = 'fenced'
    OR (
        e.state = 'exited' AND e.exit_code = 0 AND e.wrapper_identity IS NOT NULL
        AND json_type(e.packet_json, '$.contract.full_capability') = 'object'
        AND json_extract(e.packet_json, '$.contract.authority.network') = 'unrestricted'
        AND json_extract(e.packet_json, '$.contract.authority.allow_workspace_commands') = 1
    )
)
AND NOT EXISTS (
    SELECT 1 FROM executions other
    WHERE other.task_id = e.task_id AND other.effect_token != e.effect_token
        AND other.state != 'fenced'
);
