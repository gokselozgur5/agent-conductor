-- Independently observed ownership; never stores proxy bearer capabilities.
CREATE TABLE file_broker_owners (
    invocation_id TEXT PRIMARY KEY NOT NULL REFERENCES file_brokers(invocation_id),
    owner_json TEXT NOT NULL CHECK(json_valid(owner_json) AND length(owner_json)<=4096),
    authorized_at_ms INTEGER NOT NULL,
    shutdown_sha256 TEXT CHECK(shutdown_sha256 IS NULL OR length(shutdown_sha256)=64)
) STRICT;
