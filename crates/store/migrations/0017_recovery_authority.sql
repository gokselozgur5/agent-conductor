-- Bind this database to an independently retained runtime authority record.
-- A null binding is an unadopted legacy database, not proof of writer fencing.
CREATE TABLE recovery_authority (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    authority_id TEXT
);
INSERT INTO recovery_authority(singleton, authority_id) VALUES (1, NULL);
