-- Every settled native room reply is examined once for a structured delegation
-- decision. The durable outcome keeps malformed output from starving later work
-- and separates a semantic refusal from a database failure. Ordinary replies
-- remain information only; an authorized outcome grants no launch by itself.
CREATE TABLE conversation_reply_examinations (
    reply_message_id TEXT PRIMARY KEY NOT NULL REFERENCES conversation_messages(id),
    outcome TEXT NOT NULL CHECK (outcome IN ('ordinary', 'authorized', 'rejected')),
    detail TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
