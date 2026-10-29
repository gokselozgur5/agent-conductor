-- Broker records are evidence, not execution fencing or task completion.
CREATE TABLE file_brokers (
    invocation_id TEXT PRIMARY KEY NOT NULL,
    execution_token TEXT NOT NULL UNIQUE REFERENCES executions(effect_token),
    binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),
    state TEXT NOT NULL CHECK(state IN ('open','closed')),
    created_at_ms INTEGER NOT NULL,
    closed_at_ms INTEGER
) STRICT;
CREATE TABLE file_operations (
    invocation_id TEXT NOT NULL REFERENCES file_brokers(invocation_id),
    sequence INTEGER NOT NULL CHECK(sequence BETWEEN 1 AND 100000),
    request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
    result_sha256 TEXT CHECK(result_sha256 IS NULL OR length(result_sha256)=64),
    admitted_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER,
    PRIMARY KEY(invocation_id,sequence),
    CHECK((result_sha256 IS NULL)=(completed_at_ms IS NULL))
) STRICT;
CREATE UNIQUE INDEX one_pending_file_operation ON file_operations(invocation_id)
WHERE completed_at_ms IS NULL;
