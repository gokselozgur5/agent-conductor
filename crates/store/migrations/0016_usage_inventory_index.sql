-- Keep explicit usage inventory reads independent of unrelated audit history.
CREATE INDEX usage_audit_by_id ON audit_events(id)
WHERE kind = 'vendor.usage_reported';
