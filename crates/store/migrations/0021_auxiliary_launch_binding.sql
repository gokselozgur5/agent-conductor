ALTER TABLE auxiliary_invocations ADD COLUMN launch_binding_json TEXT CHECK (launch_binding_json IS NULL OR json_valid(launch_binding_json));
ALTER TABLE auxiliary_invocations ADD COLUMN process_json TEXT CHECK (process_json IS NULL OR json_valid(process_json));
ALTER TABLE auxiliary_invocations ADD COLUMN release_recorded INTEGER NOT NULL DEFAULT 0 CHECK (release_recorded IN (0,1));
ALTER TABLE auxiliary_invocations ADD COLUMN settlement_json TEXT CHECK (settlement_json IS NULL OR json_valid(settlement_json));
