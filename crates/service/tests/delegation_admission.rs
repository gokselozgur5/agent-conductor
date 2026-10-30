//! Contract parity at the transactional admission boundary; no native launches.
use agent_conductor_store::{ConversationMessage, ConversationTurnIntent, Store};
use conductor_service::{ConversationRoom, RoomConfiguration, RoomMember, parse_delegation};
use std::collections::BTreeSet;

fn topology() -> RoomConfiguration {
    RoomConfiguration {
        coordinator: "lead".into(),
        members: vec![
            RoomMember {
                agent_id: "lead".into(),
                reports_to: None,
                observes: BTreeSet::new(),
            },
            RoomMember {
                agent_id: "worker".into(),
                reports_to: Some("lead".into()),
                observes: BTreeSet::new(),
            },
        ],
    }
}

async fn seeded_reply(body: &str) -> (tempfile::TempDir, Store) {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("admission.sqlite3"))
        .await
        .unwrap();
    store
        .create_conversation("room", "/project", 1)
        .await
        .unwrap();
    store
        .configure_conversation_room("room", &serde_json::to_string(&topology()).unwrap())
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
                body: "Coordinate".into(),
                created_at_ms: 2,
            },
            2,
        )
        .await
        .unwrap();
    let digest = "a".repeat(64);
    assert!(
        store
            .reserve_conversation_turn(
                &ConversationTurnIntent {
                    message_id: "request",
                    agent_id: "lead",
                    launch_id: "launch",
                    profile_sha256: &digest,
                    prompt_sha256: &digest,
                },
                3
            )
            .await
            .unwrap()
    );
    store
        .publish_conversation_reply("launch", "reply", body, &digest, 4)
        .await
        .unwrap();
    (root, store)
}

#[tokio::test]
async fn transactional_admission_matches_the_strict_decision_contract() {
    let config = topology();
    let room = ConversationRoom::new(
        config.coordinator,
        config.members,
        &BTreeSet::from(["lead".into(), "worker".into()]),
    )
    .unwrap();
    let valid = serde_json::json!({"schema":"conductor.delegation.v1", "assignments":[
        {"agent_id":"worker", "instruction":"Inspect parser"}
    ]});
    let mut extra = valid.clone();
    extra["author"] = "lead".into();
    let mut duplicate = valid.clone();
    duplicate["assignments"]
        .as_array_mut()
        .unwrap()
        .push(valid["assignments"][0].clone());
    let mut oversized = valid.clone();
    oversized["assignments"][0]["instruction"] = "é".repeat(2049).into();
    let cases = [
        valid.to_string(),
        extra.to_string(),
        duplicate.to_string(),
        oversized.to_string(),
        valid.to_string().replace("worker", "lead"),
        valid.to_string().replace("worker", "unknown"),
        valid.to_string().replace("Inspect parser", " "),
        valid.to_string().replace("delegation.v1", "delegation.v2"),
        format!("```json\n{valid}\n```"),
        format!("{valid} {{}}"),
        r#"{"schema":"conductor.delegation.v1","assignments":[]}"#.into(),
    ];
    for body in cases {
        let expected = parse_delegation(&body, &room, "lead").is_ok();
        let (_root, store) = seeded_reply(&body).await;
        let admitted = store.authorize_conversation_delegation("reply", 5).await;
        assert_eq!(
            admitted.is_ok(),
            expected,
            "contract/admission disagreement"
        );
        if expected {
            assert_eq!(admitted.unwrap().len(), 1);
            assert_eq!(
                store
                    .authorize_conversation_delegation("reply", 6)
                    .await
                    .unwrap()
                    .len(),
                1
            );
        } else {
            assert!(
                store
                    .conversation_assignments("reply")
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        assert!(store.next_conversation_job().await.unwrap().is_none());
        assert!(store.workflows().await.unwrap().is_empty());
    }
}
