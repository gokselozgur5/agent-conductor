use agent_conductor_store::{
    ConsumerStateEffect, EVENT_SCHEMA_VERSION, EventLimits, EventSourceIdentity, EventStoreError,
    EventSubscription, NewAgent, PublishResult, SemanticEvent, Store,
};
use tempfile::TempDir;

async fn store_with_agents(agent_ids: &[&str]) -> (TempDir, Store) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("events.sqlite"))
        .await
        .expect("open store");
    for agent_id in agent_ids {
        store
            .create_agent(NewAgent {
                id: agent_id,
                adapter_kind: "test",
                protocol_fingerprint: "v1",
                capability_hash: "test",
                access_mode: "read_write",
                created_at_ms: 1,
            })
            .await
            .expect("create agent");
    }
    (directory, store)
}

fn event(id: &str, source: &str, generation: u32, causation_id: Option<&str>) -> SemanticEvent {
    let payload_json = format!(r#"{{"event":"{id}"}}"#);
    SemanticEvent {
        schema_version: EVENT_SCHEMA_VERSION,
        id: id.to_owned(),
        idempotency_key: EventSourceIdentity {
            source_agent_id: source,
            source_session_id: "session-1",
            normalized_payload: &payload_json,
        }
        .idempotency_key(),
        source_agent_id: source.to_owned(),
        source_session_id: "session-1".to_owned(),
        correlation_id: "correlation-1".to_owned(),
        causation_id: causation_id.map(str::to_owned),
        generation,
        topic: "task.changed".to_owned(),
        project_id: None,
        task_id: None,
        attempt_id: None,
        lease_epoch: None,
        path_scope: Some("/workspace/src".to_owned()),
        payload_json,
        created_at_ms: 100,
    }
}

fn subscription(id: &str, agent_id: &str) -> EventSubscription {
    EventSubscription {
        schema_version: EVENT_SCHEMA_VERSION,
        id: id.to_owned(),
        agent_id: agent_id.to_owned(),
        topic_filter: "task.*".to_owned(),
        project_id: None,
        path_prefix: Some("/workspace".to_owned()),
        echo_source: false,
        created_at_ms: 1,
    }
}

#[tokio::test]
async fn duplicate_publish_replays_without_duplicate_delivery() {
    let (_directory, store) = store_with_agents(&["source", "consumer"]).await;
    store
        .upsert_event_subscription(&subscription("sub-1", "consumer"))
        .await
        .expect("subscribe");
    let envelope = event("event-1", "source", 0, None);

    let first = store
        .publish_event(&envelope, &EventLimits::default())
        .await
        .expect("publish");
    let replay = store
        .publish_event(&envelope, &EventLimits::default())
        .await
        .expect("replay");

    assert_eq!(
        first,
        PublishResult::Published {
            event_id: "event-1".to_owned(),
            fanout: 1
        }
    );
    assert_eq!(
        replay,
        PublishResult::Duplicate {
            event_id: "event-1".to_owned()
        }
    );
    assert_eq!(
        store.event_delivery_count("event-1").await.expect("count"),
        1
    );
}

#[tokio::test]
async fn semantic_dedup_ignores_fresh_envelope_id_but_rejects_changed_content() {
    let (_directory, store) = store_with_agents(&["source", "consumer"]).await;
    store
        .upsert_event_subscription(&subscription("sub-1", "consumer"))
        .await
        .expect("subscribe");
    let original = event("event-1", "source", 0, None);
    store
        .publish_event(&original, &EventLimits::default())
        .await
        .expect("publish");

    let mut fresh_envelope = original.clone();
    fresh_envelope.id = "event-2".into();
    assert_eq!(
        store
            .publish_event(&fresh_envelope, &EventLimits::default())
            .await
            .expect("semantic replay"),
        PublishResult::Duplicate {
            event_id: "event-1".into()
        }
    );

    let mut changed = original;
    changed.payload_json = r#"{"event":"changed"}"#.into();
    assert!(matches!(
        store.publish_event(&changed, &EventLimits::default()).await,
        Err(EventStoreError::IdempotencyCollision)
    ));
}

#[tokio::test]
async fn source_echo_is_disabled_unless_explicitly_enabled() {
    let (_directory, store) = store_with_agents(&["source", "consumer"]).await;
    store
        .upsert_event_subscription(&subscription("source-sub", "source"))
        .await
        .expect("source subscription");
    store
        .upsert_event_subscription(&subscription("consumer-sub", "consumer"))
        .await
        .expect("consumer subscription");

    let published = store
        .publish_event(
            &event("event-1", "source", 0, None),
            &EventLimits::default(),
        )
        .await
        .expect("publish");

    assert_eq!(
        published,
        PublishResult::Published {
            event_id: "event-1".to_owned(),
            fanout: 1
        }
    );
    assert!(
        store
            .claim_event_delivery("source", 100, &EventLimits::default())
            .await
            .expect("source poll")
            .is_none()
    );
}

#[tokio::test]
async fn causal_generation_and_fanout_are_bounded_atomically() {
    let (_directory, store) = store_with_agents(&["source", "a", "b"]).await;
    store
        .upsert_event_subscription(&subscription("a-sub", "a"))
        .await
        .expect("subscribe a");
    store
        .upsert_event_subscription(&subscription("b-sub", "b"))
        .await
        .expect("subscribe b");
    let limits = EventLimits {
        max_fanout: 1,
        ..EventLimits::default()
    };
    assert!(matches!(
        store
            .publish_event(&event("too-wide", "source", 0, None), &limits)
            .await,
        Err(EventStoreError::FanoutLimit)
    ));
    assert_eq!(
        store.event_delivery_count("too-wide").await.expect("count"),
        0
    );

    let limits = EventLimits {
        max_fanout: 2,
        max_generation: 1,
        ..EventLimits::default()
    };
    store
        .publish_event(&event("root", "source", 0, None), &limits)
        .await
        .expect("root");
    store
        .publish_event(&event("child", "source", 1, Some("root")), &limits)
        .await
        .expect("child");
    assert!(matches!(
        store
            .publish_event(&event("cycle", "source", 2, Some("child")), &limits)
            .await,
        Err(EventStoreError::GenerationLimit)
    ));
}

#[tokio::test]
async fn unacknowledged_delivery_replays_after_lease_expiry() {
    let (_directory, store) = store_with_agents(&["source", "consumer"]).await;
    store
        .upsert_event_subscription(&subscription("sub-1", "consumer"))
        .await
        .expect("subscribe");
    let limits = EventLimits {
        delivery_lease_ms: 10,
        ..EventLimits::default()
    };
    store
        .publish_event(&event("event-1", "source", 0, None), &limits)
        .await
        .expect("publish");

    let first = store
        .claim_event_delivery("consumer", 100, &limits)
        .await
        .expect("claim")
        .expect("delivery");
    assert!(
        store
            .claim_event_delivery("consumer", 109, &limits)
            .await
            .expect("poll before expiry")
            .is_none()
    );
    let replay = store
        .claim_event_delivery("consumer", 110, &limits)
        .await
        .expect("reclaim")
        .expect("replayed delivery");
    assert_eq!(first.delivery_id, replay.delivery_id);
    assert_eq!(replay.attempts, 2);
    assert_ne!(first.claim_token, replay.claim_token);
}

#[tokio::test]
async fn stale_claim_token_and_expired_claim_cannot_ack_state() {
    let (_directory, store) = store_with_agents(&["source", "consumer"]).await;
    store
        .upsert_event_subscription(&subscription("sub-1", "consumer"))
        .await
        .expect("subscribe");
    let limits = EventLimits {
        delivery_lease_ms: 10,
        ..EventLimits::default()
    };
    store
        .publish_event(&event("event-1", "source", 0, None), &limits)
        .await
        .expect("publish");
    let first = store
        .claim_event_delivery("consumer", 100, &limits)
        .await
        .expect("claim")
        .expect("delivery");
    let effect = ConsumerStateEffect {
        namespace: "progress".into(),
        key: "consumer".into(),
        expected_version: None,
        value_json: r#"{"stale":true}"#.into(),
    };
    assert!(matches!(
        store
            .acknowledge_event_with_state(
                &first.delivery_id,
                "consumer",
                &first.claim_token,
                &effect,
                110,
            )
            .await,
        Err(EventStoreError::DeliveryNotLeased)
    ));
    let second = store
        .claim_event_delivery("consumer", 110, &limits)
        .await
        .expect("reclaim")
        .expect("delivery");
    assert!(matches!(
        store
            .acknowledge_event_with_state(
                &first.delivery_id,
                "consumer",
                &first.claim_token,
                &effect,
                111,
            )
            .await,
        Err(EventStoreError::DeliveryNotLeased)
    ));
    assert!(
        store
            .consumer_state("progress", "consumer")
            .await
            .expect("state")
            .is_none()
    );
    store
        .acknowledge_event_with_state(
            &second.delivery_id,
            "consumer",
            &second.claim_token,
            &effect,
            111,
        )
        .await
        .expect("current claim acks");
}

#[tokio::test]
async fn event_age_expiry_rejects_ack_and_state_atomically() {
    let (_directory, store) = store_with_agents(&["source", "consumer"]).await;
    store
        .upsert_event_subscription(&subscription("sub-1", "consumer"))
        .await
        .expect("subscribe");
    let limits = EventLimits {
        max_age_ms: 10,
        delivery_lease_ms: 20,
        ..EventLimits::default()
    };
    store
        .publish_event(&event("event-1", "source", 0, None), &limits)
        .await
        .expect("publish");
    let delivery = store
        .claim_event_delivery("consumer", 100, &limits)
        .await
        .expect("claim")
        .expect("delivery");
    let effect = ConsumerStateEffect {
        namespace: "progress".into(),
        key: "consumer".into(),
        expected_version: None,
        value_json: r#"{"expired":true}"#.into(),
    };
    assert!(matches!(
        store
            .acknowledge_event_with_state(
                &delivery.delivery_id,
                "consumer",
                &delivery.claim_token,
                &effect,
                110,
            )
            .await,
        Err(EventStoreError::DeliveryNotLeased)
    ));
    assert!(
        store
            .consumer_state("progress", "consumer")
            .await
            .expect("state")
            .is_none()
    );
}

#[tokio::test]
async fn state_effect_and_ack_succeed_or_roll_back_together() {
    let (_directory, store) = store_with_agents(&["source", "consumer", "intruder"]).await;
    store
        .upsert_event_subscription(&subscription("sub-1", "consumer"))
        .await
        .expect("subscribe");
    let limits = EventLimits::default();
    store
        .publish_event(&event("event-1", "source", 0, None), &limits)
        .await
        .expect("publish");
    let delivery = store
        .claim_event_delivery("consumer", 100, &limits)
        .await
        .expect("claim")
        .expect("delivery");
    let effect = ConsumerStateEffect {
        namespace: "agent-progress".to_owned(),
        key: "consumer".to_owned(),
        expected_version: None,
        value_json: r#"{"last_event":"event-1"}"#.to_owned(),
    };

    assert!(matches!(
        store
            .acknowledge_event_with_state(
                &delivery.delivery_id,
                "intruder",
                &delivery.claim_token,
                &effect,
                101,
            )
            .await,
        Err(EventStoreError::DeliveryNotLeased)
    ));
    assert!(
        store
            .consumer_state("agent-progress", "consumer")
            .await
            .expect("read state")
            .is_none()
    );

    store
        .acknowledge_event_with_state(
            &delivery.delivery_id,
            "consumer",
            &delivery.claim_token,
            &effect,
            102,
        )
        .await
        .expect("atomic ack");
    assert_eq!(
        store
            .consumer_state("agent-progress", "consumer")
            .await
            .expect("read state"),
        Some((0, r#"{"last_event":"event-1"}"#.to_owned()))
    );
    assert!(
        store
            .claim_event_delivery("consumer", 40_000, &limits)
            .await
            .expect("poll acked")
            .is_none()
    );
}
