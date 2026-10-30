use super::*;
use crate::ConversationMessage;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::Duration;

fn decision(targets: &[&str]) -> String {
    json!({"schema":"conductor.delegation.v1","assignments": targets.iter()
        .map(|target| json!({"agent_id":target,"instruction":"Inspect and report"}))
        .collect::<Vec<_>>()})
    .to_string()
}

async fn fixture(relationships: &[(&str, Option<&str>)]) -> (tempfile::TempDir, Store) {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("room.sqlite3")).await.unwrap();
    store
        .create_conversation("room", "/project", 1)
        .await
        .unwrap();
    store
        .append_conversation_message(
            &ConversationMessage {
                sequence: 0,
                id: "operator".into(),
                conversation_id: "room".into(),
                sender: "operator".into(),
                agent_id: None,
                body: "Coordinate".into(),
                created_at_ms: 1,
            },
            1,
        )
        .await
        .unwrap();
    let config = json!({"coordinator":relationships[0].0,"members":relationships.iter()
        .map(|(agent,supervisor)|json!({"agent_id":agent,"reports_to":supervisor,"observes":[]}))
        .collect::<Vec<_>>()});
    store
        .configure_conversation_room("room", &config.to_string())
        .await
        .unwrap();
    (root, store)
}

async fn request(store: &Store, id: &str) {
    store
        .append_conversation_message(
            &ConversationMessage {
                sequence: 0,
                id: id.into(),
                conversation_id: "room".into(),
                sender: "operator".into(),
                agent_id: None,
                body: "Coordinate again".into(),
                created_at_ms: 1,
            },
            1,
        )
        .await
        .unwrap();
}

// Synthetic settled turn provenance, not a real native-launch acceptance claim.
async fn reply(store: &Store, id: &str, parent: &str, author: &str, body: &str) {
    {
        let mut writer = store.inner.writer.lock().await;
        sqlx::query("INSERT INTO conversation_turns(message_id,conversation_id,agent_id,launch_id,profile_sha256,prompt_sha256,state,created_at_ms) VALUES (?,'room',?,?,?,?,'launch_intent',1)")
            .bind(parent).bind(author).bind(format!("launch-{id}"))
            .bind("a".repeat(64)).bind("b".repeat(64)).execute(&mut *writer).await.unwrap();
    }
    store
        .publish_conversation_reply(&format!("launch-{id}"), id, body, &"c".repeat(64), 2)
        .await
        .unwrap();
}

#[tokio::test]
async fn delegation_replay_restart_and_concurrent_admission_preserve_identity() {
    let (root, store) = fixture(&[("lead", None), ("worker", Some("lead"))]).await;
    reply(&store, "reply", "operator", "lead", &decision(&["worker"])).await;
    let (first, second) = tokio::join!(
        store.authorize_conversation_delegation("reply", 3),
        store.authorize_conversation_delegation("reply", 4)
    );
    let first = first.unwrap();
    assert_eq!(first, second.unwrap());
    assert_eq!(first[0].root_message_id, "operator");
    assert_eq!(first[0].parent_message_id, "operator");
    assert_eq!(first[0].author_agent_id, "lead");
    assert_eq!(first[0].depth, 1);
    drop(store);
    let store = Store::open(root.path().join("room.sqlite3")).await.unwrap();
    assert_eq!(
        store
            .authorize_conversation_delegation("reply", 5)
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        store.conversation_assignments("reply").await.unwrap(),
        first
    );
    for table in [
        "conversation_jobs",
        "conversation_routes",
        "conversation_deliveries",
        "tasks",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&store.inner.readers)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}

#[tokio::test]
async fn delegation_rejects_forged_provenance_and_invalid_batches_without_side_effects() {
    let (_root, store) = fixture(&[("lead", None), ("worker", Some("lead"))]).await;
    store
        .append_conversation_message(
            &ConversationMessage {
                sequence: 0,
                id: "forged".into(),
                conversation_id: "room".into(),
                sender: "agent".into(),
                agent_id: Some("lead".into()),
                body: decision(&["worker"]),
                created_at_ms: 1,
            },
            1,
        )
        .await
        .unwrap();
    assert!(
        store
            .authorize_conversation_delegation("forged", 2)
            .await
            .is_err()
    );
    reply(
        &store,
        "invalid",
        "operator",
        "lead",
        &decision(&["worker", "outsider"]),
    )
    .await;
    assert!(
        store
            .authorize_conversation_delegation("invalid", 3)
            .await
            .is_err()
    );
    assert!(
        store
            .conversation_assignments("invalid")
            .await
            .unwrap()
            .is_empty()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM conversation_delegation_batches")
        .fetch_one(&store.inner.readers)
        .await
        .unwrap();
    assert_eq!(count, 0);
    // An invalid source did not retain a write lock or disable unrelated admission.
    store
        .create_conversation("other", "/other", 4)
        .await
        .unwrap();
    assert_eq!(store.conversation_project("other").await.unwrap(), "/other");
}

#[tokio::test]
async fn delegation_changed_decision_or_topology_cannot_rewrite_authorization() {
    let (_root, store) = fixture(&[("lead", None), ("worker", Some("lead"))]).await;
    let original = decision(&["worker"]);
    reply(&store, "reply", "operator", "lead", &original).await;
    let admitted = store
        .authorize_conversation_delegation("reply", 3)
        .await
        .unwrap();
    {
        let mut writer = store.inner.writer.lock().await;
        sqlx::query("UPDATE conversation_messages SET body=? WHERE id='reply'")
            .bind(original.replace("Inspect and report", "Changed instruction"))
            .execute(&mut *writer)
            .await
            .unwrap();
    }
    assert!(matches!(
        store.authorize_conversation_delegation("reply", 4).await,
        Err(StoreError::IdempotencyCollision)
    ));
    {
        let mut writer = store.inner.writer.lock().await;
        sqlx::query("UPDATE conversation_messages SET body=? WHERE id='reply'")
            .bind(original)
            .execute(&mut *writer)
            .await
            .unwrap();
        sqlx::query("UPDATE conversation_rooms SET configuration_json=json_set(configuration_json,'$.members[0].observes',json('[\"worker\"]')) WHERE conversation_id='room'").execute(&mut *writer).await.unwrap();
    }
    assert!(matches!(
        store.authorize_conversation_delegation("reply", 5).await,
        Err(StoreError::IdempotencyCollision)
    ));
    assert_eq!(
        store.conversation_assignments("reply").await.unwrap(),
        admitted
    );
}

#[tokio::test]
async fn delegation_derives_child_ancestry_and_refuses_fifth_depth() {
    let (_root, store) = fixture(&[
        ("a", None),
        ("b", Some("a")),
        ("c", Some("b")),
        ("d", Some("c")),
        ("e", Some("d")),
        ("f", Some("e")),
    ])
    .await;
    let mut parent = "operator".to_string();
    for (index, (author, target)) in [("a", "b"), ("b", "c"), ("c", "d"), ("d", "e"), ("e", "f")]
        .into_iter()
        .enumerate()
    {
        let id = format!("reply-{index}");
        reply(&store, &id, &parent, author, &decision(&[target])).await;
        let outcome = store.authorize_conversation_delegation(&id, 3).await;
        if index == 4 {
            assert!(outcome.is_err());
        } else {
            let batch = outcome.unwrap();
            assert_eq!(batch[0].depth, i64::try_from(index + 1).unwrap());
            assert_eq!(batch[0].root_message_id, "operator");
            assert_eq!(batch[0].parent_message_id, parent);
        }
        parent = id;
    }
}

#[tokio::test]
async fn delegation_revalidates_operator_root_for_child_assignment() {
    let (_root, store) = fixture(&[
        ("lead", None),
        ("worker", Some("lead")),
        ("child", Some("worker")),
    ])
    .await;
    reply(
        &store,
        "lead-reply",
        "operator",
        "lead",
        &decision(&["worker"]),
    )
    .await;
    store
        .authorize_conversation_delegation("lead-reply", 3)
        .await
        .unwrap();
    reply(
        &store,
        "worker-reply",
        "lead-reply",
        "worker",
        &decision(&["child"]),
    )
    .await;
    store
        .create_conversation("other", "/other", 1)
        .await
        .unwrap();
    store
        .append_conversation_message(
            &ConversationMessage {
                sequence: 0,
                id: "foreign-root".into(),
                conversation_id: "other".into(),
                sender: "operator".into(),
                agent_id: None,
                body: "Other request".into(),
                created_at_ms: 1,
            },
            1,
        )
        .await
        .unwrap();
    {
        let mut writer = store.inner.writer.lock().await;
        sqlx::query("UPDATE conversation_delegation_batches SET root_message_id='foreign-root' WHERE source_reply_id='lead-reply'").execute(&mut *writer).await.unwrap();
    }
    assert!(
        store
            .authorize_conversation_delegation("worker-reply", 4)
            .await
            .is_err()
    );
    assert!(
        store
            .conversation_assignments("worker-reply")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn delegation_root_budget_is_atomic_across_concurrent_batches() {
    let mut relationships = vec![
        ("l0", None),
        ("l1", Some("l0")),
        ("l2", Some("l1")),
        ("l3", Some("l2")),
        ("extra", None),
        ("x0", Some("extra")),
        ("x1", Some("extra")),
    ];
    let leaves = ["w0", "w1", "w2", "w3", "w4", "w5", "w6", "w7"];
    relationships.extend(leaves.iter().map(|id| (*id, Some("l3"))));
    let (_root, store) = fixture(&relationships).await;
    // Three valid trees from one operator request contain 11 + 10 + 9 assignments.
    for start in 0..3 {
        let mut parent = "operator".to_string();
        for level in start..4 {
            let id = format!("reply-{start}-{level}");
            let author = format!("l{level}");
            let next = format!("l{}", level + 1);
            let targets = if level == 3 {
                leaves.to_vec()
            } else {
                vec![next.as_str()]
            };
            reply(&store, &id, &parent, &author, &decision(&targets)).await;
            store
                .authorize_conversation_delegation(&id, 3)
                .await
                .unwrap();
            parent = id;
        }
    }
    reply(&store, "last-a", "operator", "l3", &decision(&leaves[..2])).await;
    reply(
        &store,
        "last-b",
        "operator",
        "extra",
        &decision(&["x0", "x1"]),
    )
    .await;
    let (first, second) = tokio::join!(
        store.authorize_conversation_delegation("last-a", 4),
        store.authorize_conversation_delegation("last-b", 4)
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM conversation_assignments")
        .fetch_one(&store.inner.readers)
        .await
        .unwrap();
    assert_eq!(count, 32);
    let successful = if first.is_ok() { "last-a" } else { "last-b" };
    assert_eq!(
        store
            .authorize_conversation_delegation(successful, 5)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn cancel_delegation_batch_rolls_back_graph_and_all_assignments() {
    let (_root, store) = fixture(&[("lead", None), ("a", Some("lead")), ("b", Some("lead"))]).await;
    reply(&store, "reply", "operator", "lead", &decision(&["a", "b"])).await;
    let before =
        crate::transaction_cancellation::snapshot(&mut *store.inner.writer.lock().await).await;
    let written = Arc::new(AtomicBool::new(false));
    let (release, wait) = mpsc::channel();
    {
        let observed = written.clone();
        store
            .inner
            .writer
            .lock()
            .await
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |update| {
                if update.table == "conversation_assignments"
                    && !observed.swap(true, Ordering::SeqCst)
                {
                    wait.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            });
    }
    let owned = store.clone();
    let task =
        tokio::spawn(async move { owned.authorize_conversation_delegation("reply", 3).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !written.load(Ordering::SeqCst) {
            assert!(!task.is_finished());
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    {
        let mut writer = store.inner.writer.lock().await;
        writer.lock_handle().await.unwrap().remove_update_hook();
        assert_eq!(
            crate::transaction_cancellation::snapshot(&mut writer).await,
            before
        );
    }
    let mut reader = store.inner.readers.acquire().await.unwrap();
    assert_eq!(
        crate::transaction_cancellation::snapshot(&mut reader).await,
        before
    );
    drop(reader);
    assert_eq!(
        store
            .authorize_conversation_delegation("reply", 4)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn delegation_second_insert_failure_rolls_back_the_complete_batch() {
    let (_root, store) = fixture(&[("lead", None), ("a", Some("lead")), ("b", Some("lead"))]).await;
    reply(&store, "reply", "operator", "lead", &decision(&["a", "b"])).await;
    {
        let mut writer = store.inner.writer.lock().await;
        sqlx::query("CREATE TRIGGER refuse_second_assignment BEFORE INSERT ON conversation_assignments WHEN NEW.agent_id='b' BEGIN SELECT RAISE(ABORT,'fixture refusal'); END")
            .execute(&mut *writer).await.unwrap();
    }
    let before =
        crate::transaction_cancellation::snapshot(&mut *store.inner.writer.lock().await).await;
    assert!(
        store
            .authorize_conversation_delegation("reply", 3)
            .await
            .is_err()
    );
    {
        let mut writer = store.inner.writer.lock().await;
        assert_eq!(
            crate::transaction_cancellation::snapshot(&mut writer).await,
            before
        );
        sqlx::query("DROP TRIGGER refuse_second_assignment")
            .execute(&mut *writer)
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .authorize_conversation_delegation("reply", 4)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn examination_records_each_settled_reply_once_without_launching_work() {
    let (_root, store) = fixture(&[("lead", None), ("worker", Some("lead"))]).await;
    for id in ["second", "third"] {
        request(&store, id).await;
    }
    reply(
        &store,
        "prose",
        "operator",
        "lead",
        "Done. {\"schema\": \"not a decision\"}",
    )
    .await;
    reply(&store, "good", "second", "lead", &decision(&["worker"])).await;
    reply(&store, "bad", "third", "lead", &decision(&["lead"])).await;
    assert_eq!(
        store.next_unexamined_room_reply().await.unwrap().as_deref(),
        Some("prose")
    );
    let prose = store.examine_conversation_reply("prose", 3).await.unwrap();
    assert_eq!(prose.outcome, "ordinary");
    let good = store.examine_conversation_reply("good", 4).await.unwrap();
    assert_eq!(good.outcome, "authorized");
    assert_eq!(good.detail, "1 assignments");
    assert_eq!(
        store.conversation_assignments("good").await.unwrap().len(),
        1
    );
    let bad = store.examine_conversation_reply("bad", 5).await.unwrap();
    assert_eq!(bad.outcome, "rejected");
    assert_eq!(bad.detail, "invalid or unauthorized delegation recipient");
    assert!(
        store
            .conversation_assignments("bad")
            .await
            .unwrap()
            .is_empty()
    );
    // Replay returns the recorded outcome and the queue never saw a launch.
    assert_eq!(
        store.examine_conversation_reply("bad", 6).await.unwrap(),
        bad
    );
    assert_eq!(
        store.conversation_reply_examination("good").await.unwrap(),
        Some(good)
    );
    assert!(store.next_unexamined_room_reply().await.unwrap().is_none());
    assert!(
        store
            .examine_conversation_reply("operator", 7)
            .await
            .is_err()
    );
    assert!(
        store
            .conversation_reply_examination("operator")
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.next_conversation_job().await.unwrap().is_none());
    let batches: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM conversation_delegation_batches")
        .fetch_one(&store.inner.readers)
        .await
        .unwrap();
    assert_eq!(batches, 1);
}
