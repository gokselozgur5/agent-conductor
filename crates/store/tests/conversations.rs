use agent_conductor_store::{ConversationMessage, Store, StoreError};

fn message(id: &str, conversation: &str) -> ConversationMessage {
    ConversationMessage {
        sequence: 999,
        id: id.into(),
        conversation_id: conversation.into(),
        sender: "operator".into(),
        agent_id: None,
        body: "Connect the agents and preserve this request.".into(),
        created_at_ms: 999,
    }
}

#[tokio::test]
async fn conversation_replay_restart_and_project_binding() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state.sqlite3");
    let store = Store::open(&path).await.unwrap();
    store
        .create_conversation("one", "/project", 1)
        .await
        .unwrap();
    store
        .create_conversation("one", "/project", 2)
        .await
        .unwrap();
    assert!(matches!(
        store.create_conversation("one", "/other", 2).await,
        Err(StoreError::IdempotencyCollision)
    ));
    let input = message("request", "one");
    let saved = store.append_conversation_message(&input, 3).await.unwrap();
    assert_eq!(saved.created_at_ms, 3);
    assert_ne!(saved.sequence, input.sequence);
    let replay = store.append_conversation_message(&input, 4).await.unwrap();
    assert!(saved == replay);
    let mut conflict = input.clone();
    conflict.body = "Different request".into();
    assert!(matches!(
        store.append_conversation_message(&conflict, 5).await,
        Err(StoreError::IdempotencyCollision)
    ));
    drop(store);
    let reopened = Store::open(&path).await.unwrap();
    let rows = reopened.conversation_messages("one", 0).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0] == saved);
}

#[tokio::test]
async fn conversation_pagination_isolated_and_sender_validated() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    for id in ["one", "two"] {
        store.create_conversation(id, "/project", 1).await.unwrap();
    }
    for i in 0..103 {
        store
            .append_conversation_message(&message(&format!("m{i}"), "one"), i)
            .await
            .unwrap();
    }
    store
        .append_conversation_message(&message("other", "two"), 200)
        .await
        .unwrap();
    let first = store.conversation_messages("one", 0).await.unwrap();
    assert_eq!(first.len(), 100);
    let rest = store
        .conversation_messages("one", first.last().unwrap().sequence)
        .await
        .unwrap();
    assert_eq!(rest.len(), 3);
    assert!(
        first
            .iter()
            .chain(&rest)
            .all(|m| m.conversation_id == "one")
    );
    let mut invalid = message("invalid", "one");
    invalid.sender = "agent".into();
    assert!(
        store
            .append_conversation_message(&invalid, 201)
            .await
            .is_err()
    );
    invalid.agent_id = Some("claude-worker".into());
    store
        .append_conversation_message(&invalid, 202)
        .await
        .unwrap();
    assert!(
        store
            .append_conversation_message(&message("missing", "unknown"), 203)
            .await
            .is_err()
    );
    invalid.id = "oversized".into();
    invalid.body = "x".repeat(65_537);
    assert!(
        store
            .append_conversation_message(&invalid, 204)
            .await
            .is_err()
    );
}
