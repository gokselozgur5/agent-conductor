-- Native evidence is recorded by the trusted runtime, never synthesized by migration.
CREATE TABLE legacy_auxiliary_host_witness (
    task_id TEXT PRIMARY KEY NOT NULL REFERENCES tasks(id),
    reason TEXT NOT NULL,
    marker_generation INTEGER NOT NULL,
    authority_id TEXT NOT NULL,
    platform TEXT NOT NULL,
    host_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    observed_generation INTEGER NOT NULL,
    checked_generation INTEGER NOT NULL,
    ownership_json TEXT NOT NULL CHECK(json_valid(ownership_json)),
    status TEXT NOT NULL CHECK(status IN ('observed','contradiction','settled')),
    settlement_json TEXT CHECK(settlement_json IS NULL OR json_valid(settlement_json))
) STRICT;
