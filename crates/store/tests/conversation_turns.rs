use agent_conductor_store::{ConversationMessage, ConversationTurnIntent, Store, StoreError};

#[tokio::test]
async fn durable_launch_intent_is_not_reissued_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state.sqlite3");
    let store = Store::open(&path).await.unwrap();
    store
        .create_conversation("chat", "/project", 1)
        .await
        .unwrap();
    for id in ["one", "two"] {
        store
            .append_conversation_message(
                &ConversationMessage {
                    sequence: 0,
                    id: id.into(),
                    conversation_id: "chat".into(),
                    sender: "operator".into(),
                    agent_id: None,
                    body: "Run this".into(),
                    created_at_ms: 0,
                },
                2,
            )
            .await
            .unwrap();
    }
    let profile = "a".repeat(64);
    let prompt = "b".repeat(64);
    let intent = ConversationTurnIntent {
        message_id: "one",
        agent_id: "codex",
        launch_id: "launch-one",
        profile_sha256: &profile,
        prompt_sha256: &prompt,
    };
    let (first, concurrent) = tokio::join!(
        store.reserve_conversation_turn(&intent, 3),
        store.reserve_conversation_turn(&intent, 3)
    );
    assert_ne!(first.unwrap(), concurrent.unwrap());
    drop(store);
    let reopened = Store::open(&path).await.unwrap();
    assert!(
        !reopened
            .reserve_conversation_turn(&intent, 4)
            .await
            .unwrap()
    );
    let changed = ConversationTurnIntent {
        profile_sha256: &prompt,
        ..intent
    };
    assert!(matches!(
        reopened.reserve_conversation_turn(&changed, 5).await,
        Err(StoreError::IdempotencyCollision)
    ));
    let overlapping = ConversationTurnIntent {
        message_id: "two",
        launch_id: "launch-two",
        ..changed
    };
    assert!(
        reopened
            .reserve_conversation_turn(&overlapping, 6)
            .await
            .is_err()
    );
    assert!(
        !reopened
            .reserve_conversation_turn(
                &ConversationTurnIntent {
                    message_id: "one",
                    agent_id: "codex",
                    launch_id: "launch-one",
                    profile_sha256: &profile,
                    prompt_sha256: &prompt,
                },
                7
            )
            .await
            .unwrap()
    );
    // A failed reply insert cannot release the active turn.
    assert!(
        reopened
            .publish_conversation_reply("launch-one", "one", "Reply", &profile, 8)
            .await
            .is_err()
    );
    assert!(
        reopened
            .reserve_conversation_turn(&overlapping, 9)
            .await
            .is_err()
    );
    reopened
        .publish_conversation_reply("launch-one", "reply-one", "Reply", &profile, 10)
        .await
        .unwrap();
    reopened
        .publish_conversation_reply("launch-one", "reply-one", "Reply", &profile, 11)
        .await
        .unwrap();
    assert!(matches!(
        reopened
            .publish_conversation_reply("launch-one", "reply-one", "Changed", &profile, 12)
            .await,
        Err(StoreError::IdempotencyCollision)
    ));
    let rows = reopened.conversation_messages("chat", 0).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].sender, "agent");
    assert_eq!(rows[2].agent_id.as_deref(), Some("codex"));
    assert!(
        reopened
            .reserve_conversation_turn(&overlapping, 13)
            .await
            .unwrap()
    );
}
