-- Correction round of a delegation batch: 0 for a decision made on an operator
-- request or assignment, n for the n-th decision made from a continuation.
ALTER TABLE conversation_delegation_batches
    ADD COLUMN round INTEGER NOT NULL DEFAULT 0 CHECK (round BETWEEN 0 AND 2);
