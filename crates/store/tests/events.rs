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

#[tokio::test]
async fn hook_baseline_and_deliveries_commit_together_or_not_at_all() {
    use agent_conductor_store::RepositoryObservation;
    let (directory, store) = store_with_agents(&["source", "consumer"]).await;
    let limits = EventLimits::default();
    store
        .upsert_event_subscription(&subscription("watch-sub", "consumer"))
        .await
        .unwrap();
    let mut observation = RepositoryObservation {
        id: "watch",
        config_json: "{}",
        expected_version: None,
        snapshot_json: "{\"before\":true}",
        now_ms: 100,
    };
    store
        .publish_repository_observation(&observation, &[], &limits)
        .await
        .unwrap();
    observation.expected_version = Some(0);
    observation.snapshot_json = "{\"after\":true}";
    let good = event("watch-1", "source", 0, None);
    let bad = event("watch-2", "missing-source", 0, None);
    assert!(
        store
            .publish_repository_observation(&observation, &[good.clone(), bad], &limits)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .repository_observation("watch")
            .await
            .unwrap()
            .unwrap(),
        ("{}".into(), 0, "{\"before\":true}".into())
    );
    assert!(
        store
            .claim_event_delivery("consumer", 100, &limits)
            .await
            .unwrap()
            .is_none()
    );
    store
        .publish_repository_observation(&observation, std::slice::from_ref(&good), &limits)
        .await
        .unwrap();
    // A replay with a stale baseline cannot publish extra events or advance it.
    assert!(matches!(
        store
            .publish_repository_observation(&observation, &[good], &limits)
            .await,
        Err(EventStoreError::ConsumerStateConflict)
    ));
    let reopened = Store::open(directory.path().join("events.sqlite"))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .repository_observation("watch")
            .await
            .unwrap()
            .unwrap(),
        ("{}".into(), 1, "{\"after\":true}".into())
    );
    assert!(
        reopened
            .claim_event_delivery("consumer", 100, &limits)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        reopened
            .claim_event_delivery("consumer", 100, &limits)
            .await
            .unwrap()
            .is_none()
    );
    observation.expected_version = Some(1);
    observation.config_json = "{\"other_root\":true}";
    assert!(matches!(
        reopened
            .publish_repository_observation(&observation, &[], &limits)
            .await,
        Err(EventStoreError::ConsumerStateConflict)
    ));
}

#[tokio::test]
async fn subscribed_workflow_wake_is_atomic_once_only_and_survives_restart() {
    let (_directory, store) = store_with_agents(&["source", "next:worker", "wrong"]).await;
    let boot = store.start_runtime(1).await.unwrap().boot_generation;
    store
        .upsert_event_subscription(&subscription("wake", "next:worker"))
        .await
        .unwrap();
    let plan = r#"{"wake_subscription":"wake"}"#;
    assert!(store.enqueue_workflow("wrong", plan, 2).await.is_err());
    assert!(store.workflows().await.unwrap().is_empty());
    store.enqueue_workflow("next", plan, 2).await.unwrap();
    store.enqueue_workflow("next", plan, 3).await.unwrap();
    assert!(store.claim_workflow(boot, 100).await.unwrap().is_none());
    let limits = EventLimits::default();
    store
        .publish_event(&event("self", "next:worker", 0, None), &limits)
        .await
        .unwrap();
    assert!(store.claim_workflow(boot, 100).await.unwrap().is_none());
    store
        .publish_event(&event("external", "source", 0, None), &limits)
        .await
        .unwrap();
    let (left, right) = tokio::join!(
        store.claim_workflow(boot, 101),
        store.claim_workflow(boot, 101)
    );
    assert_eq!(
        usize::from(left.unwrap().is_some()) + usize::from(right.unwrap().is_some()),
        1
    );
    let trigger = store.workflow_trigger("next").await.unwrap().unwrap();
    let value: serde_json::Value = serde_json::from_str(&trigger).unwrap();
    assert_eq!(value["id"], "external");
    assert_eq!(value["correlation_id"], "correlation-1");
    assert!(
        store
            .claim_event_delivery("next:worker", 102, &limits)
            .await
            .unwrap()
            .is_none()
    );
    let restarted = store.start_runtime(103).await.unwrap().boot_generation;
    store
        .recover_workflow_dispatch(restarted, 104)
        .await
        .unwrap();
    assert!(
        store
            .claim_workflow(restarted, 105)
            .await
            .unwrap()
            .is_none()
    );
    store.requeue_workflow("next", 1, 106).await.unwrap();
    assert!(
        store
            .claim_workflow(restarted, 107)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store.workflow_trigger("next").await.unwrap().unwrap(),
        trigger
    );
}

#[tokio::test]
async fn stale_or_exhausted_events_do_not_wake_or_block_unrelated_work() {
    let (_directory, store) = store_with_agents(&["source", "next:worker"]).await;
    let boot = store.start_runtime(1).await.unwrap().boot_generation;
    store
        .upsert_event_subscription(&subscription("wake", "next:worker"))
        .await
        .unwrap();
    store
        .enqueue_workflow("next", r#"{"wake_subscription":"wake"}"#, 2)
        .await
        .unwrap();
    let limits = EventLimits {
        max_age_ms: 10,
        max_retries: 1,
        ..EventLimits::default()
    };
    store
        .publish_event(&event("old", "source", 0, None), &limits)
        .await
        .unwrap();
    assert!(store.claim_workflow(boot, 110).await.unwrap().is_none());
    let mut fresh = event("exhausted", "source", 0, None);
    fresh.created_at_ms = 111;
    store.publish_event(&fresh, &limits).await.unwrap();
    store
        .claim_event_delivery("next:worker", 111, &limits)
        .await
        .unwrap()
        .unwrap();
    assert!(store.claim_workflow(boot, 112).await.unwrap().is_none());
    store.enqueue_workflow("ordinary", "{}", 113).await.unwrap();
    assert_eq!(
        store
            .claim_workflow(boot, 114)
            .await
            .unwrap()
            .unwrap()
            .task_id,
        "ordinary"
    );
    assert!(store.workflow_trigger("next").await.unwrap().is_none());
    assert!(
        store
            .workflows()
            .await
            .unwrap()
            .iter()
            .find(|w| w.task_id == "next")
            .unwrap()
            .waiting_for_event
    );
}

#[tokio::test]
async fn wake_generation_limit_and_failed_claim_preserve_pending_delivery() {
    use sqlx::Connection;
    let (directory, store) = store_with_agents(&["source", "next:worker"]).await;
    let boot = store.start_runtime(1).await.unwrap().boot_generation;
    let limits = EventLimits::default();
    for generation in 0..8 {
        let parent = (generation > 0).then(|| format!("chain-{}", generation - 1));
        store
            .publish_event(
                &event(
                    &format!("chain-{generation}"),
                    "source",
                    generation,
                    parent.as_deref(),
                ),
                &limits,
            )
            .await
            .unwrap();
    }
    store
        .upsert_event_subscription(&subscription("wake", "next:worker"))
        .await
        .unwrap();
    store
        .enqueue_workflow("next", r#"{"wake_subscription":"wake"}"#, 2)
        .await
        .unwrap();
    store
        .publish_event(&event("chain-8", "source", 8, Some("chain-7")), &limits)
        .await
        .unwrap();
    assert!(store.claim_workflow(boot, 101).await.unwrap().is_none());
    store
        .publish_event(&event("eligible", "source", 0, None), &limits)
        .await
        .unwrap();
    let mut db = sqlx::SqliteConnection::connect(&format!(
        "sqlite://{}",
        directory.path().join("events.sqlite").display()
    ))
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER fail_wake BEFORE UPDATE OF status ON workflow_dispatch WHEN NEW.status='running' BEGIN SELECT RAISE(ABORT,'injected'); END").execute(&mut db).await.unwrap();
    assert!(store.claim_workflow(boot, 102).await.is_err());
    assert!(store.workflow_trigger("next").await.unwrap().is_none());
    let status: String =
        sqlx::query_scalar("SELECT status FROM event_deliveries WHERE event_id='eligible'")
            .fetch_one(&mut db)
            .await
            .unwrap();
    assert_eq!(status, "pending");
    sqlx::query("DROP TRIGGER fail_wake")
        .execute(&mut db)
        .await
        .unwrap();
    assert!(store.claim_workflow(boot, 103).await.unwrap().is_some());
    assert!(
        store
            .workflow_trigger("next")
            .await
            .unwrap()
            .unwrap()
            .contains("eligible")
    );
}
