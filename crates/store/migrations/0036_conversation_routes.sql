-- One immutable recipient snapshot per source message. Notification delivery
-- does not grant native execution or alter managed task acceptance.
CREATE TABLE conversation_routes (
    message_id TEXT PRIMARY KEY NOT NULL REFERENCES conversation_messages(id),
    recipients_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE TABLE conversation_deliveries (
    message_id TEXT NOT NULL REFERENCES conversation_routes(message_id),
    agent_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('assignment', 'information')),
    reason TEXT NOT NULL,
    acknowledged_at_ms INTEGER,
    PRIMARY KEY (message_id, agent_id)
);
CREATE INDEX pending_conversation_deliveries
ON conversation_deliveries(agent_id, message_id) WHERE acknowledged_at_ms IS NULL;
CREATE TABLE conversation_rooms (
    conversation_id TEXT PRIMARY KEY NOT NULL REFERENCES conversations(id),
    configuration_json TEXT NOT NULL
);
