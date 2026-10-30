CREATE TABLE conversation_jobs (
    message_id TEXT PRIMARY KEY NOT NULL REFERENCES conversation_messages(id),
    agent_profile_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'blocked')),
    created_at_ms INTEGER NOT NULL
);
