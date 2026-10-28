-- An operator-approved workflow may wait for one subscribed event.
CREATE TABLE workflow_wakes (
    task_id TEXT PRIMARY KEY REFERENCES workflow_dispatch(task_id),
    subscription_id TEXT NOT NULL REFERENCES event_subscriptions(id),
    delivery_id TEXT UNIQUE REFERENCES event_deliveries(id),
    trigger_json TEXT CHECK(trigger_json IS NULL OR json_valid(trigger_json))
) STRICT;
