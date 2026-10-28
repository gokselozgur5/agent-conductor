ALTER TABLE semantic_events ADD COLUMN attempt_id TEXT;
ALTER TABLE semantic_events ADD COLUMN lease_epoch INTEGER CHECK (lease_epoch IS NULL OR lease_epoch > 0);

ALTER TABLE event_deliveries ADD COLUMN claim_token TEXT;
CREATE UNIQUE INDEX event_delivery_claim_token
ON event_deliveries(claim_token)
WHERE claim_token IS NOT NULL;
