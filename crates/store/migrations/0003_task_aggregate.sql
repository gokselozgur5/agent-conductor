ALTER TABLE tasks
ADD COLUMN aggregate_json TEXT
CHECK (aggregate_json IS NULL OR json_valid(aggregate_json));
