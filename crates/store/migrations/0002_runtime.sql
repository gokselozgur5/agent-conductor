CREATE TABLE runtime_metadata (
    singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
    boot_generation INTEGER NOT NULL CHECK (boot_generation >= 0),
    last_started_at_ms INTEGER NOT NULL
) STRICT;

INSERT INTO runtime_metadata (singleton, boot_generation, last_started_at_ms)
VALUES (1, 0, 0);
