CREATE TABLE conversation_turns (
    message_id TEXT PRIMARY KEY NOT NULL REFERENCES conversation_messages(id),
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    agent_id TEXT NOT NULL,
    launch_id TEXT UNIQUE NOT NULL,
    profile_sha256 TEXT NOT NULL,
    prompt_sha256 TEXT NOT NULL,
    reply_message_id TEXT UNIQUE REFERENCES conversation_messages(id),
    output_sha256 TEXT,
    state TEXT NOT NULL CHECK (state IN ('launch_intent', 'replied', 'failed', 'unknown')),
    created_at_ms INTEGER NOT NULL
);
-- Unknown outcomes retain the conversation until explicitly reconciled.
CREATE UNIQUE INDEX conversation_active_turn ON conversation_turns(conversation_id)
WHERE state IN ('launch_intent', 'unknown');
