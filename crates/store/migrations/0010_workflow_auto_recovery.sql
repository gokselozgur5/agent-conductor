ALTER TABLE workflow_dispatch ADD COLUMN auto_recovery_count INTEGER NOT NULL DEFAULT 0 CHECK(auto_recovery_count BETWEEN 0 AND 1);
