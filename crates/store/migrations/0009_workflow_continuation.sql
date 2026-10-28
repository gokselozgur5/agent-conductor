ALTER TABLE workflow_dispatch ADD COLUMN resume_version INTEGER CHECK(resume_version >= 0);
