-- Expected wrapper identity is durable intent, never evidence of ACK or release.
-- Existing executions remain unknown; do not infer identity from process absence.
ALTER TABLE executions ADD COLUMN expected_wrapper_identity TEXT;
