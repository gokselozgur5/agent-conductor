-- Authoritative authorization is separate from message prose and information
-- routes. Each batch is derived from one settled native reply under a stored room.
CREATE TABLE conversation_delegation_batches (
    source_reply_id TEXT PRIMARY KEY NOT NULL REFERENCES conversation_messages(id),
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    root_message_id TEXT NOT NULL REFERENCES conversation_messages(id),
    parent_message_id TEXT NOT NULL REFERENCES conversation_messages(id),
    author_agent_id TEXT NOT NULL,
    depth INTEGER NOT NULL CHECK (depth BETWEEN 1 AND 4),
    decision_json TEXT NOT NULL,
    room_configuration_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX conversation_delegation_root
ON conversation_delegation_batches(root_message_id);
CREATE TABLE conversation_assignments (
    source_reply_id TEXT NOT NULL REFERENCES conversation_delegation_batches(source_reply_id),
    agent_id TEXT NOT NULL,
    instruction TEXT NOT NULL,
    PRIMARY KEY (source_reply_id, agent_id)
);
