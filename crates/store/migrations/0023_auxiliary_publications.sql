-- New native results link to their exact settled invocation. Historical/manual
-- results are not backfilled with invented process evidence.
CREATE TABLE auxiliary_publications (
    invocation_id TEXT PRIMARY KEY NOT NULL REFERENCES auxiliary_invocations(invocation_id),
    audit_id TEXT NOT NULL UNIQUE REFERENCES audit_events(id),
    actor_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL
) STRICT;
