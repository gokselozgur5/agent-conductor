-- Eligibility for checks/review of a copied source, not workspace reassignment.
-- Unrestricted normal exits remain unfenced in the authoritative execution row.
CREATE VIEW capture_eligible_executions AS
SELECT e.* FROM executions e
WHERE (
    e.state = 'fenced'
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
