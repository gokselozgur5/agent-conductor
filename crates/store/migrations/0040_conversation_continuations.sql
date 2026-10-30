-- One supervisor continuation per delegation batch, opened only after every
-- assigned recipient settled (replied, failed or blocked). It admits exactly one
-- native turn for the batch author on its own decision message; it grants no
-- further delegation and never marks managed tasks complete.
CREATE TABLE conversation_continuations (
    source_reply_id TEXT PRIMARY KEY NOT NULL
        REFERENCES conversation_delegation_batches(source_reply_id),
    agent_id TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
