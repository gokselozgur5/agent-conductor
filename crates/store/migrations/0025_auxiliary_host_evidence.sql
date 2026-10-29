-- Admission evidence is inserted in the same transaction as the invocation.
-- Missing historical evidence is never backfilled by migration.
CREATE TABLE auxiliary_host_evidence (
    invocation_id TEXT PRIMARY KEY NOT NULL REFERENCES auxiliary_invocations(invocation_id),
    kind TEXT NOT NULL CHECK(kind IN ('admission','recorded_process','recovery_observation')),
    host_json TEXT NOT NULL CHECK(json_valid(host_json)),
    ownership_json TEXT CHECK(ownership_json IS NULL OR json_valid(ownership_json))
) STRICT;
