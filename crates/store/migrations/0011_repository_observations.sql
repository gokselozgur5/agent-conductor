CREATE TABLE repository_observations (
    id TEXT PRIMARY KEY NOT NULL,
    config_json TEXT NOT NULL CHECK(json_valid(config_json)),
    version INTEGER NOT NULL CHECK(version >= 0),
    snapshot_json TEXT NOT NULL CHECK(json_valid(snapshot_json)),
    updated_at_ms INTEGER NOT NULL
) STRICT;
