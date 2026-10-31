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

#[tokio::test]
async fn examination_returns_database_failures_instead_of_recording_them() {
    let (_root, store) = fixture(&[("lead", None), ("worker", Some("lead"))]).await;
    reply(&store, "reply", "operator", "lead", &decision(&["worker"])).await;
    {
        let mut writer = store.inner.writer.lock().await;
        sqlx::query("CREATE TRIGGER refuse_batch BEFORE INSERT ON conversation_delegation_batches BEGIN SELECT RAISE(ABORT,'fixture failure'); END")
            .execute(&mut *writer).await.unwrap();
    }
    assert!(matches!(
        store.examine_conversation_reply("reply", 3).await,
        Err(StoreError::Database(_))
    ));
    assert!(
        store
            .conversation_reply_examination("reply")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.next_unexamined_room_reply().await.unwrap().as_deref(),
        Some("reply")
    );
    {
        let mut writer = store.inner.writer.lock().await;
        sqlx::query("DROP TRIGGER refuse_batch")
            .execute(&mut *writer)
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .examine_conversation_reply("reply", 4)
            .await
            .unwrap()
            .outcome,
        "authorized"
    );
}

#[tokio::test]
async fn examination_rolls_back_authorization_when_cancelled_at_the_record_write() {
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
                if update.table == "conversation_reply_examinations"
                    && !observed.swap(true, Ordering::SeqCst)
                {
                    wait.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            });
    }
    let owned = store.clone();
    let task = tokio::spawn(async move { owned.examine_conversation_reply("reply", 3).await });
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
            .examine_conversation_reply("reply", 4)
            .await
            .unwrap()
            .outcome,
        "authorized"
    );
    assert_eq!(
        store.conversation_assignments("reply").await.unwrap().len(),
        2
    );
}

/// A settled coordinator decision naming worker and ghost, already authorized.
async fn authorized_pair() -> (tempfile::TempDir, Store) {
    let (root, store) = fixture(&[
        ("lead", None),
        ("worker", Some("lead")),
        ("ghost", Some("lead")),
    ])
    .await;
    reply(
        &store,
        "reply",
        "operator",
        "lead",
        &decision(&["worker", "ghost"]),
    )
    .await;
    assert!(
        store
            .next_unadmitted_conversation_assignment()
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .conversation_assignment("reply", "worker")
            .await
            .unwrap()
            .is_none()
    );
    store
        .authorize_conversation_delegation("reply", 3)
        .await
        .unwrap();
    (root, store)
}

fn profile(id: &str) -> String {
    json!({"id": id}).to_string()
}

#[tokio::test]
async fn queue_admits_only_the_exact_authorized_recipient() {
    let (_root, store) = authorized_pair().await;
    assert_eq!(
        store
            .next_unadmitted_conversation_assignment()
            .await
            .unwrap(),
        Some(("reply".into(), "ghost".into()))
    );
    let worker = store
        .conversation_assignment("reply", "worker")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(worker.instruction, "Inspect and report");
    assert_eq!(worker.author_agent_id, "lead");
    assert!(
        store
            .conversation_assignment("reply", "lead")
            .await
            .unwrap()
            .is_none()
    );
    for refused in ["lead", "outsider"] {
        assert!(
            store
                .queue_conversation_job("reply", &profile(refused), 4)
                .await
                .is_err()
        );
    }
    store
        .queue_conversation_job("reply", &profile("worker"), 4)
        .await
        .unwrap();
    assert_eq!(
        store
            .next_unadmitted_conversation_assignment()
            .await
            .unwrap(),
        Some(("reply".into(), "ghost".into()))
    );
    assert_eq!(
        store.next_conversation_job().await.unwrap(),
        Some(("reply".into(), profile("worker")))
    );
}

#[tokio::test]
async fn blocked_recipient_is_visible_and_never_enters_the_queue() {
    let (_root, store) = authorized_pair().await;
    for refused in ["lead", "outsider"] {
        assert!(
            store
                .block_conversation_recipient("reply", refused, 4)
                .await
                .is_err()
        );
    }
    store
        .queue_conversation_job("reply", &profile("worker"), 4)
        .await
        .unwrap();
    store
        .block_conversation_recipient("reply", "ghost", 4)
        .await
        .unwrap();
    store
        .block_conversation_recipient("reply", "ghost", 5)
        .await
        .unwrap();
    // A blocked recipient never replaces a queued one.
    store
        .block_conversation_recipient("reply", "worker", 5)
        .await
        .unwrap();
    assert!(
        store
            .next_unadmitted_conversation_assignment()
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.next_conversation_job().await.unwrap(),
        Some(("reply".into(), profile("worker")))
    );
    let states: Vec<(String, String)> =
        sqlx::query_as("SELECT agent_id,state FROM conversation_jobs ORDER BY agent_id")
            .fetch_all(&store.inner.readers)
            .await
            .unwrap();
    assert_eq!(
        states,
        vec![
            ("ghost".into(), "blocked".into()),
            ("worker".into(), "queued".into())
        ]
    );
}

#[tokio::test]
async fn reservation_admits_only_the_exact_authorized_recipient() {
    let (_root, store) = authorized_pair().await;
    let digest = "d".repeat(64);
    let intent = |agent: &'static str| crate::ConversationTurnIntent {
        message_id: "reply",
        agent_id: agent,
        launch_id: if agent == "worker" {
            "worker-launch"
        } else {
            "lead-launch"
        },
        profile_sha256: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        prompt_sha256: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
    };
    assert!(
        store
            .reserve_conversation_turn(&intent("lead"), 6)
            .await
            .is_err()
    );
    assert!(
        store
            .reserve_conversation_turn(&intent("worker"), 6)
            .await
            .unwrap()
    );
    assert!(
        !store
            .reserve_conversation_turn(&intent("worker"), 7)
            .await
            .unwrap()
    );
    store
        .publish_conversation_reply("worker-launch", "worker-reply", "Inspected", &digest, 8)
        .await
        .unwrap();
    // The worker's reply inherits the authorized ancestry when it delegates further.
    assert!(
        store
            .conversation_reply_origin("worker-reply")
            .await
            .unwrap()
            .is_some_and(
                |origin| origin.parent_message_id == "reply" && origin.author_agent_id == "worker"
            )
    );
}

/// Settles one recipient of the authorized pair with an ordinary reply.
async fn settle(store: &Store, agent: &str) {
    reply(store, &format!("{agent}-reply"), "reply", agent, "Done").await;
    store
        .examine_conversation_reply(&format!("{agent}-reply"), 5)
        .await
        .unwrap();
}

#[tokio::test]
async fn continuation_waits_for_every_recipient_and_opens_once() {
    let (_root, store) = authorized_pair().await;
    assert!(store.next_ready_continuation().await.unwrap().is_none());
    assert!(
        store
            .open_conversation_continuation("reply", 4)
            .await
            .is_err()
    );
    settle(&store, "worker").await;
    assert!(store.next_ready_continuation().await.unwrap().is_none());
    store
        .block_conversation_recipient("reply", "ghost", 6)
        .await
        .unwrap();
    assert_eq!(
        store.next_ready_continuation().await.unwrap(),
        Some(("reply".into(), "lead".into()))
    );
    assert!(
        !store
            .conversation_continuation("reply", "lead")
            .await
            .unwrap()
    );
    for now in [7, 8] {
        let author = store.open_conversation_continuation("reply", now).await;
        assert_eq!(author.unwrap(), "lead");
    }
    assert!(store.next_ready_continuation().await.unwrap().is_none());
    assert!(
        store
            .conversation_continuation("reply", "lead")
            .await
            .unwrap()
    );
    assert!(
        !store
            .conversation_continuation("reply", "worker")
            .await
            .unwrap()
    );
}

async fn settle_turn(store: &Store, agent: &str, state: &str) {
    let mut writer = store.inner.writer.lock().await;
    sqlx::query("INSERT INTO conversation_turns(message_id,conversation_id,agent_id,launch_id,profile_sha256,prompt_sha256,state,created_at_ms) VALUES ('reply','room',?,?,?,?,?,5)")
        .bind(agent).bind(format!("launch-{agent}")).bind("a".repeat(64))
        .bind("b".repeat(64)).bind(state).execute(&mut *writer).await.unwrap();
}

#[tokio::test]
async fn unknown_outcomes_never_settle_a_recipient_but_failures_do() {
    let (_root, store) = authorized_pair().await;
    settle(&store, "worker").await;
    settle_turn(&store, "ghost", "unknown").await;
    assert!(store.next_ready_continuation().await.unwrap().is_none());
    assert!(
        store
            .open_conversation_continuation("reply", 6)
            .await
            .is_err()
    );
    {
        let mut writer = store.inner.writer.lock().await;
        sqlx::query("UPDATE conversation_turns SET state='failed' WHERE agent_id='ghost'")
            .execute(&mut *writer)
            .await
            .unwrap();
    }
    assert_eq!(
        store.next_ready_continuation().await.unwrap(),
        Some(("reply".into(), "lead".into()))
    );
}

#[tokio::test]
async fn a_recipient_that_delegated_settles_after_its_own_continuation() {
    let (_root, store) = fixture(&[
        ("lead", None),
        ("worker", Some("lead")),
        ("child", Some("worker")),
    ])
    .await;
    reply(&store, "reply", "operator", "lead", &decision(&["worker"])).await;
    store.examine_conversation_reply("reply", 3).await.unwrap();
    reply(
        &store,
        "worker-reply",
        "reply",
        "worker",
        &decision(&["child"]),
    )
    .await;
    let nested = store.examine_conversation_reply("worker-reply", 4).await;
    assert_eq!(nested.unwrap().outcome, "authorized");
    reply(&store, "child-reply", "worker-reply", "child", "Done").await;
    store
        .examine_conversation_reply("child-reply", 5)
        .await
        .unwrap();
    // Only the inner batch is ready; the lead waits for the worker's summary.
    assert_eq!(
        store.next_ready_continuation().await.unwrap(),
        Some(("worker-reply".into(), "worker".into()))
    );
    store
        .open_conversation_continuation("worker-reply", 6)
        .await
        .unwrap();
    assert!(store.next_ready_continuation().await.unwrap().is_none());
    reply(
        &store,
        "worker-summary",
        "worker-reply",
        "worker",
        "Summary",
    )
    .await;
    assert_eq!(
        store.next_ready_continuation().await.unwrap(),
        Some(("reply".into(), "lead".into()))
    );
}

#[tokio::test]
async fn only_an_opened_continuation_admits_the_author_on_its_decision() {
    let (_root, store) = authorized_pair().await;
    settle(&store, "worker").await;
    settle(&store, "ghost").await;
    let intent = crate::ConversationTurnIntent {
        message_id: "reply",
        agent_id: "lead",
        launch_id: "continuation-launch",
        profile_sha256: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        prompt_sha256: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
    };
    assert!(
        store
            .queue_conversation_job("reply", &profile("lead"), 6)
            .await
            .is_err()
    );
    assert!(store.reserve_conversation_turn(&intent, 6).await.is_err());
    store
        .open_conversation_continuation("reply", 7)
        .await
        .unwrap();
    assert!(
        store
            .queue_conversation_job("reply", &profile("worker"), 8)
            .await
            .is_ok()
    );
    store
        .queue_conversation_job("reply", &profile("lead"), 8)
        .await
        .unwrap();
    assert!(store.reserve_conversation_turn(&intent, 9).await.unwrap());
    assert!(!store.reserve_conversation_turn(&intent, 10).await.unwrap());
}

#[tokio::test]
async fn a_blocked_nested_continuation_still_settles_the_parent_batch() {
    let (_root, store) = fixture(&[
        ("lead", None),
        ("worker", Some("lead")),
        ("child", Some("worker")),
    ])
    .await;
    reply(&store, "reply", "operator", "lead", &decision(&["worker"])).await;
    store.examine_conversation_reply("reply", 3).await.unwrap();
    reply(
        &store,
        "worker-reply",
        "reply",
        "worker",
        &decision(&["child"]),
    )
    .await;
    store
        .examine_conversation_reply("worker-reply", 4)
        .await
        .unwrap();
    reply(&store, "child-reply", "worker-reply", "child", "Done").await;
    store
        .examine_conversation_reply("child-reply", 5)
        .await
        .unwrap();
    store
        .open_conversation_continuation("worker-reply", 6)
        .await
        .unwrap();
    // The worker's profile is gone, so its continuation is blocked, not run.
    store
        .block_conversation_recipient("worker-reply", "worker", 7)
        .await
        .unwrap();
    assert_eq!(
        store.next_ready_continuation().await.unwrap(),
        Some(("reply".into(), "lead".into()))
    );
}

/// Runs `rounds` continuations of the lead, each replying with a new decision.
async fn correct(store: &Store, rounds: usize) -> Vec<ConversationReplyExamination> {
    let (mut parent, mut outcomes) = ("reply".to_string(), Vec::new());
    for round in 0..rounds {
        let (done, next) = (format!("worker-{round}"), format!("correction-{round}"));
        reply(store, &done, &parent, "worker", "Done").await;
        store.examine_conversation_reply(&done, 5).await.unwrap();
        store
            .open_conversation_continuation(&parent, 6)
            .await
            .unwrap();
        reply(store, &next, &parent, "lead", &decision(&["worker"])).await;
        outcomes.push(store.examine_conversation_reply(&next, 7).await.unwrap());
        parent = next;
    }
    outcomes
}

#[tokio::test]
async fn continuations_may_correct_twice_and_the_third_round_is_refused() {
    let (_root, store) = fixture(&[("lead", None), ("worker", Some("lead"))]).await;
    reply(&store, "reply", "operator", "lead", &decision(&["worker"])).await;
    store.examine_conversation_reply("reply", 3).await.unwrap();
    let outcomes = correct(&store, 3).await;
    assert_eq!(outcomes[0].outcome, "authorized");
    assert_eq!(outcomes[1].outcome, "authorized");
    assert_eq!(outcomes[2].outcome, "rejected");
    assert_eq!(outcomes[2].detail, "correction round budget exhausted");
    // Corrections keep the original root and depth and advance only the round.
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT root_message_id,depth,round FROM conversation_delegation_batches ORDER BY round",
    )
    .fetch_all(&store.inner.readers)
    .await
    .unwrap();
    let expected = |round| ("operator".to_string(), 1, round);
    assert_eq!(rows, vec![expected(0), expected(1), expected(2)]);
}

#[tokio::test]
async fn routing_shows_each_recipient_with_its_reason_and_state() {
    let (_root, store) = authorized_pair().await;
    let notice = crate::ConversationRecipient {
        agent_id: "lead".into(),
        kind: "information".into(),
        reason: "report_to_supervisor".into(),
    };
    store
        .route_conversation_message("reply", &[notice], 4)
        .await
        .unwrap();
    store
        .queue_conversation_job("reply", &profile("worker"), 4)
        .await
        .unwrap();
    store
        .block_conversation_recipient("reply", "ghost", 4)
        .await
        .unwrap();
    let row = |agent: &str, kind: &str, reason: &str, state: &str| crate::ConversationRouting {
        agent_id: agent.into(),
        kind: kind.into(),
        reason: reason.into(),
        state: state.into(),
    };
    assert_eq!(
        store.conversation_message_routing("reply").await.unwrap(),
        vec![
            row("ghost", "delegated", "delegated_by_supervisor", "blocked"),
            row("worker", "delegated", "delegated_by_supervisor", "queued"),
            row("lead", "information", "report_to_supervisor", "pending"),
        ]
    );
    store
        .acknowledge_conversation_delivery("reply", "lead", 5)
        .await
        .unwrap();
    let routing = store.conversation_message_routing("reply").await.unwrap();
    assert_eq!(routing[2].state, "received");
}

#[tokio::test]
async fn an_unroutable_reply_is_settled_with_a_visible_reason_and_unblocks_the_queue() {
    let (_root, store) = fixture(&[("lead", None)]).await;
    reply(&store, "ghost-reply", "operator", "ghost", "Stray output").await;
    request(&store, "second").await;
    reply(&store, "lead-reply", "second", "lead", "Real output").await;
    let head = store.next_unrouted_room_reply().await.unwrap();
    assert_eq!(head.unwrap().0, "ghost-reply");
    store
        .reject_unroutable_conversation_reply("ghost-reply", "author is not a room member", 5)
        .await
        .unwrap();
    // Replay keeps the first reason and the queue moves on to the next reply.
    store
        .reject_unroutable_conversation_reply("ghost-reply", "another reason", 6)
        .await
        .unwrap();
    let head = store.next_unrouted_room_reply().await.unwrap();
    assert_eq!(head.unwrap().0, "lead-reply");
    let examination = store
        .conversation_reply_examination("ghost-reply")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(examination.outcome, "rejected");
    assert_eq!(examination.detail, "author is not a room member");
    assert!(
        store
            .next_unexamined_room_reply()
            .await
            .unwrap()
            .is_some_and(|id| id == "lead-reply")
    );
    assert!(
        store
            .reject_unroutable_conversation_reply("operator", "operator text", 7)
            .await
            .is_err()
    );
}
