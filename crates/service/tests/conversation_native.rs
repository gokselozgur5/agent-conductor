#![cfg(unix)]
use agent_conductor_store::{ConversationMessage, Store};
use conductor_runtime::{AdapterKind, ExecutionAccount, VendorPin};
use conductor_service::{ConversationAgent, PersistentControlService};
use sha2::{Digest, Sha256};
use std::os::unix::fs::PermissionsExt;

#[tokio::test]
async fn native_conversation_publishes_real_process_output_once_and_passes_context() {
    let root = tempfile::tempdir().unwrap();
    let (worker, agent) = fixture_agent(root.path());
    let store = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    store
        .create_conversation("chat", root.path().to_str().unwrap(), 1)
        .await
        .unwrap();
    let service = PersistentControlService::start(store.clone(), 1)
        .await
        .unwrap();
    let (_cancel, receiver) = tokio::sync::watch::channel(false);
    service
        .configure_conversation_agents(vec![agent.clone()])
        .unwrap();
    for (id, body) in [
        ("one", "First operator request"),
        ("two", "Use the previous reply"),
    ] {
        store
            .append_conversation_message(
                &ConversationMessage {
                    sequence: 0,
                    id: id.into(),
                    conversation_id: "chat".into(),
                    sender: "operator".into(),
                    agent_id: None,
                    body: body.into(),
                    created_at_ms: 0,
                },
                2,
            )
            .await
            .unwrap();
        service
            .queue_conversation_turn(id, &agent.id)
            .await
            .unwrap();
        service
            .queue_conversation_turn(id, &agent.id)
            .await
            .unwrap();
        assert!(
            service
                .dispatch_next_conversation(receiver.clone())
                .await
                .unwrap()
        );
        assert!(
            !service
                .dispatch_next_conversation(receiver.clone())
                .await
                .unwrap()
        );
        assert!(
            !service
                .run_conversation_message(id, &agent, receiver.clone())
                .await
                .unwrap()
        );
    }
    let args = std::fs::read_to_string(root.path().join("received-arguments")).unwrap();
    assert!(args.contains("--yolo"));
    assert!(args.contains("First operator request"));
    assert!(args.contains("Native fixture reply"));
    let messages = store.conversation_messages("chat", 0).await.unwrap();
    assert_eq!(messages.len(), 4);
    assert_eq!(messages[3].body, "Native fixture reply");
    drop(service);
    let restarted = PersistentControlService::start(store.clone(), 2)
        .await
        .unwrap();
    assert!(
        !restarted
            .run_conversation_message("one", &agent, receiver.clone())
            .await
            .unwrap()
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("launches"))
            .unwrap()
            .lines()
            .count(),
        2
    );
    assert_failed_turn_not_retried(&restarted, &store, &worker, agent, receiver).await;
}

async fn assert_failed_turn_not_retried(
    restarted: &PersistentControlService,
    store: &Store,
    worker: &std::path::Path,
    agent: ConversationAgent,
    receiver: tokio::sync::watch::Receiver<bool>,
) {
    let root = worker.parent().unwrap();
    // A failed native turn keeps its reservation and is not silently retried.
    std::fs::write(
        worker,
        "#!/bin/sh\nprintf 'failed-launch\\n' >> launches\nexit 1\n",
    )
    .unwrap();
    let mut failing = agent;
    failing.vendor.executable_sha256 =
        format!("{:x}", Sha256::digest(std::fs::read(worker).unwrap()));
    store
        .append_conversation_message(
            &ConversationMessage {
                sequence: 0,
                id: "three".into(),
                conversation_id: "chat".into(),
                sender: "operator".into(),
                agent_id: None,
                body: "A failed turn".into(),
                created_at_ms: 0,
            },
            3,
        )
        .await
        .unwrap();
    assert!(
        restarted
            .run_conversation_message("three", &failing, receiver.clone())
            .await
            .is_err()
    );
    assert!(
        !restarted
            .run_conversation_message("three", &failing, receiver)
            .await
            .unwrap()
    );
    assert_eq!(
        std::fs::read_to_string(root.join("launches"))
            .unwrap()
            .lines()
            .count(),
        3
    );
}

fn fixture_agent(root: &std::path::Path) -> (std::path::PathBuf, ConversationAgent) {
    let worker = root.join("native-fixture");
    std::fs::write(&worker, r#"#!/bin/sh
set -eu
printf 'launch\n' >> launches
printf '%s\n' "$@" > received-arguments
printf '%s\n' '{"type":"thread.started","thread_id":"fixture"}' '{"type":"turn.started"}' '{"type":"item.completed","item":{"type":"agent_message","text":"Native fixture reply"}}' '{"type":"turn.completed","usage":{}}'
"#).unwrap();
    std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o700)).unwrap();
    let account = ExecutionAccount::resolve(&std::env::var("USER").unwrap()).unwrap();
    let agent = ConversationAgent {
        id: "codex-fixture".into(),
        account,
        vendor: VendorPin {
            kind: AdapterKind::Codex,
            version: "fixture".into(),
            executable: worker.clone(),
            executable_sha256: format!("{:x}", Sha256::digest(std::fs::read(&worker).unwrap())),
            conformance_sha256: "a".repeat(64),
        },
        path: "/usr/bin:/bin".into(),
        timeout_ms: 10_000,
    };
    (worker, agent)
}
