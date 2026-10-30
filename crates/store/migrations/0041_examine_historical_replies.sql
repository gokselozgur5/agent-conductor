-- Replies settled before schema 39 were never examined for delegation. Record
-- them as ordinary information so an upgrade cannot authorize and launch work
-- from historical decisions that nobody reviewed under the new contract.
INSERT INTO conversation_reply_examinations(reply_message_id,outcome,detail,created_at_ms)
SELECT m.id,'ordinary','settled before reply examination',m.created_at_ms
FROM conversation_messages m
JOIN conversation_turns t ON t.reply_message_id=m.id AND t.state='replied'
WHERE m.sender='agent'
  AND NOT EXISTS(SELECT 1 FROM conversation_reply_examinations e WHERE e.reply_message_id=m.id);
