-- Conversation text is context, never task completion or execution authority.
CREATE TABLE conversations (
    id TEXT PRIMARY KEY NOT NULL,
    project_path TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE TABLE conversation_messages (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT UNIQUE NOT NULL,
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    sender TEXT NOT NULL CHECK (sender IN ('operator', 'agent')),
    agent_id TEXT,
    body TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    CHECK ((sender = 'operator' AND agent_id IS NULL) OR
           (sender = 'agent' AND agent_id IS NOT NULL))
);
CREATE INDEX conversation_message_page ON conversation_messages(conversation_id, sequence);
