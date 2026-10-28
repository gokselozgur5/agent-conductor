-- Parent-observed native birth metadata, committed with ACK before worker release.
-- NULL is legacy/unbound evidence, never permission to assume a process is absent.
ALTER TABLE executions ADD COLUMN process_json TEXT CHECK (process_json IS NULL OR json_valid(process_json));
