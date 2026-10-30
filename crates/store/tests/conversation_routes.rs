use agent_conductor_store::{ConversationMessage, ConversationRecipient, Store, StoreError};

#[tokio::test]
async fn routing_survives_restart_and_cannot_change_or_redeliver_acknowledged_messages() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("room.sqlite3");
    let store = Store::open(&path).await.unwrap();
    store
        .create_conversation("room", "/project", 1)
        .await
        .unwrap();
    store
        .append_conversation_message(
            &ConversationMessage {
                sequence: 0,
                id: "request".into(),
                conversation_id: "room".into(),
                sender: "operator".into(),
                agent_id: None,
                body: "Coordinate this work".into(),
                created_at_ms: 0,
            },
            2,
        )
        .await
        .unwrap();
    let recipients = vec![
        ConversationRecipient {
            agent_id: "lead".into(),
            kind: "assignment".into(),
            reason: "operator_request".into(),
        },
        ConversationRecipient {
            agent_id: "reviewer".into(),
            kind: "information".into(),
            reason: "dependency_update".into(),
        },
    ];
    store
        .route_conversation_message("request", &recipients, 3)
        .await
        .unwrap();
    assert_eq!(
        store.conversation_message_status("request").await.unwrap(),
        "queued"
    );
    store
        .acknowledge_conversation_delivery("request", "lead", 4)
        .await
        .unwrap();
    drop(store);
    let store = Store::open(&path).await.unwrap();
    let mut reordered = recipients.clone();
    reordered.reverse();
    store
        .route_conversation_message("request", &reordered, 5)
        .await
        .unwrap();
    assert!(
        store
            .pending_conversation_deliveries("room", "lead")
            .await
            .unwrap()
            .is_empty()
    );
    let pending = store
        .pending_conversation_deliveries("room", "reviewer")
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].kind, "information");
    assert!(
        store
            .pending_conversation_deliveries("other", "reviewer")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store.route_conversation_message("request", &[], 6).await,
        Err(StoreError::IdempotencyCollision)
    ));
    assert_eq!(
        store
            .pending_conversation_deliveries("room", "reviewer")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .acknowledge_conversation_delivery("request", "outsider", 7)
            .await
            .is_err()
    );
    assert!(
        store
            .route_conversation_message("missing", &recipients, 8)
            .await
            .is_err()
    );
}
