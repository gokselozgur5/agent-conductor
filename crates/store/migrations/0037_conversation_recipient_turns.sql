-- Preserve all previous launches while allowing one source message to address
-- multiple agents. Unknown outcomes still block the room, never grant retries.
DROP INDEX conversation_active_turn;
ALTER TABLE conversation_turns RENAME TO conversation_turns_legacy;
CREATE TABLE conversation_turns (
    message_id TEXT NOT NULL REFERENCES conversation_messages(id),
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    agent_id TEXT NOT NULL,
    launch_id TEXT UNIQUE NOT NULL,
    profile_sha256 TEXT NOT NULL,
    prompt_sha256 TEXT NOT NULL,
    reply_message_id TEXT UNIQUE REFERENCES conversation_messages(id),
    output_sha256 TEXT,
    state TEXT NOT NULL CHECK (state IN ('launch_intent', 'replied', 'failed', 'unknown')),
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (message_id, agent_id)
);
INSERT INTO conversation_turns SELECT * FROM conversation_turns_legacy;
DROP TABLE conversation_turns_legacy;
CREATE UNIQUE INDEX conversation_active_turn ON conversation_turns(conversation_id)
WHERE state IN ('launch_intent', 'unknown');
ALTER TABLE conversation_jobs RENAME TO conversation_jobs_legacy;
CREATE TABLE conversation_jobs (
    message_id TEXT NOT NULL REFERENCES conversation_messages(id),
    agent_id TEXT NOT NULL,
    agent_profile_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'blocked')),
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (message_id, agent_id)
);
INSERT INTO conversation_jobs SELECT message_id,json_extract(agent_profile_json,'$.id'),agent_profile_json,state,created_at_ms FROM conversation_jobs_legacy;
DROP TABLE conversation_jobs_legacy;
