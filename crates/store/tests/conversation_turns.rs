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

#[tokio::test]
async fn reply_origin_rejects_inconsistent_native_bindings() {
    use sqlx::Connection;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("origin.sqlite3");
    let store = Store::open(&path).await.unwrap();
    for room in ["room", "other"] {
        store
            .create_conversation(room, "/project", 1)
            .await
            .unwrap();
    }
    store
        .append_conversation_message(
            &ConversationMessage {
                sequence: 0,
                id: "request".into(),
                conversation_id: "room".into(),
                sender: "operator".into(),
                agent_id: None,
                body: "Work".into(),
                created_at_ms: 1,
            },
            1,
        )
        .await
        .unwrap();
    let digest = "a".repeat(64);
    store
        .reserve_conversation_turn(
            &ConversationTurnIntent {
                message_id: "request",
                agent_id: "lead",
                launch_id: "launch",
                profile_sha256: &digest,
                prompt_sha256: &digest,
            },
            2,
        )
        .await
        .unwrap();
    store
        .publish_conversation_reply("launch", "reply", "Report", &digest, 3)
        .await
        .unwrap();
    let mut db = sqlx::SqliteConnection::connect(path.to_str().unwrap())
        .await
        .unwrap();
    // Corrupt each binding independently to discriminate the provenance predicates.
    for (corrupt, restore) in [
        (
            "UPDATE conversation_turns SET state='unknown'",
            "UPDATE conversation_turns SET state='replied'",
        ),
        (
            "UPDATE conversation_messages SET agent_id='other-agent' WHERE id='reply'",
            "UPDATE conversation_messages SET agent_id='lead' WHERE id='reply'",
        ),
        (
            "UPDATE conversation_messages SET conversation_id='other' WHERE id='reply'",
            "UPDATE conversation_messages SET conversation_id='room' WHERE id='reply'",
        ),
    ] {
        sqlx::query(corrupt).execute(&mut db).await.unwrap();
        assert!(
            store
                .conversation_reply_origin("reply")
                .await
                .unwrap()
                .is_none()
        );
        sqlx::query(restore).execute(&mut db).await.unwrap();
        let origin = store
            .conversation_reply_origin("reply")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(origin.author_agent_id, "lead");
        assert_eq!(origin.parent_message_id, "request");
        assert_eq!(origin.conversation_id, "room");
    }
}
