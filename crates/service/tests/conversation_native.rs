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

#[tokio::test]
async fn shared_room_fans_out_and_automatically_informs_the_next_agent() {
    let root = tempfile::tempdir().unwrap();
    let (_, first) = fixture_agent(root.path());
    let mut second = first.clone();
    second.id = "reviewer".into();
    let store = seeded_room(root.path(), "room.sqlite3").await;
    let service = PersistentControlService::start(store.clone(), 1)
        .await
        .unwrap();
    service
        .configure_conversation_agents(vec![first.clone(), second.clone()])
        .unwrap();
    service
        .configure_room("room", shared_configuration(&first.id, &second.id))
        .await
        .unwrap();
    service
        .route_room_request("shared-request", &[], true)
        .await
        .unwrap();
    drop(service);
    // Recover routing admitted before process loss, without a second submission.
    let service = PersistentControlService::start(store.clone(), 2)
        .await
        .unwrap();
    service
        .configure_conversation_agents(vec![first, second])
        .unwrap();
    let (_cancel, receiver) = tokio::sync::watch::channel(false);
    drain(&service, &receiver).await;
    assert_eq!(launches(root.path()), 2);
    let messages = store.conversation_messages("room", 0).await.unwrap();
    assert_eq!(messages.len(), 3);
    assert_ne!(messages[1].agent_id, messages[2].agent_id);
    let args = std::fs::read_to_string(root.path().join("received-arguments")).unwrap();
    assert!(args.contains("Native fixture reply"));
    assert!(args.contains("dependency_update"));
    assert!(
        store
            .pending_conversation_deliveries("room", "reviewer")
            .await
            .unwrap()
            .iter()
            .all(|d| d.kind != "information")
    );
    service
        .route_room_request("shared-request", &[], true)
        .await
        .unwrap();
    assert!(!service.dispatch_next_conversation(receiver).await.unwrap());
}

fn shared_configuration(lead: &str, worker: &str) -> conductor_service::RoomConfiguration {
    use conductor_service::{RoomConfiguration, RoomMember};
    RoomConfiguration {
        coordinator: lead.into(),
        members: vec![
            RoomMember {
                agent_id: lead.into(),
                reports_to: None,
                observes: std::collections::BTreeSet::new(),
            },
            RoomMember {
                agent_id: worker.into(),
                reports_to: Some(lead.into()),
                observes: [lead.into()].into(),
            },
        ],
    }
}

#[tokio::test]
async fn room_rpc_retains_topology_and_refuses_authority_changes() {
    use conductor_protocol::{Request, Response};
    use conductor_service::ControlService;
    let root = tempfile::tempdir().unwrap();
    let (_, first) = fixture_agent(root.path());
    let mut second = first.clone();
    second.id = "reviewer".into();
    let store = Store::open(root.path().join("rpc.sqlite3")).await.unwrap();
    store
        .create_conversation("room", "/project", 1)
        .await
        .unwrap();
    let service = PersistentControlService::start(store.clone(), 1)
        .await
        .unwrap();
    service
        .configure_conversation_agents(vec![first.clone(), second.clone()])
        .unwrap();
    let read = Request::ConversationRoom {
        conversation_id: "room".into(),
    };
    assert_eq!(
        service.handle(read.clone()).await.unwrap(),
        Response::ConversationRoom {
            configuration: None
        }
    );
    let configuration = serde_json::to_value(shared_configuration(&first.id, &second.id)).unwrap();
    let request = Request::ConfigureConversationRoom {
        conversation_id: "room".into(),
        configuration: configuration.clone(),
    };
    service.handle(request.clone()).await.unwrap();
    service.handle(request).await.unwrap();
    assert!(
        service
            .handle(Request::ConfigureConversationRoom {
                conversation_id: "room".into(),
                configuration: serde_json::to_value(shared_configuration(&second.id, &first.id))
                    .unwrap(),
            })
            .await
            .is_err()
    );
    drop(service);
    let service = PersistentControlService::start(store, 2).await.unwrap();
    assert_eq!(
        service.handle(read).await.unwrap(),
        Response::ConversationRoom {
            configuration: Some(configuration)
        }
    );
    assert!(
        service
            .handle(Request::RouteConversationMessage {
                message_id: "missing".into(),
                recipients: vec![],
                everyone: true,
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn delegation_proposal_requires_native_provenance_and_never_queues_work() {
    use agent_conductor_store::ConversationTurnIntent;
    let root = tempfile::tempdir().unwrap();
    let (_, lead) = fixture_agent(root.path());
    let mut worker = lead.clone();
    worker.id = "worker".into();
    let store = Store::open(root.path().join("delegation.sqlite3"))
        .await
        .unwrap();
    store
        .create_conversation("room", "/project", 1)
        .await
        .unwrap();
    let body = r#"{"schema":"conductor.delegation.v1","assignments":[{"agent_id":"worker","instruction":"Inspect the parser"}]}"#;
    for (id, sender, agent_id) in [
        ("request", "operator", None),
        ("unbound", "agent", Some(lead.id.clone())),
    ] {
        store
            .append_conversation_message(
                &ConversationMessage {
                    sequence: 0,
                    id: id.into(),
                    conversation_id: "room".into(),
                    sender: sender.into(),
                    agent_id,
                    body: body.into(),
                    created_at_ms: 2,
                },
                2,
            )
            .await
            .unwrap();
    }
    let service = PersistentControlService::start(store.clone(), 1)
        .await
        .unwrap();
    service
        .configure_conversation_agents(vec![lead.clone(), worker.clone()])
        .unwrap();
    service
        .configure_room("room", shared_configuration(&lead.id, &worker.id))
        .await
        .unwrap();
    for id in ["missing", "request", "unbound"] {
        assert!(service.inspect_native_delegation(id).await.is_err());
    }
    let digest = "a".repeat(64);
    assert!(
        store
            .reserve_conversation_turn(
                &ConversationTurnIntent {
                    message_id: "request",
                    agent_id: &lead.id,
                    launch_id: "source-launch",
                    profile_sha256: &digest,
                    prompt_sha256: &digest,
                },
                3
            )
            .await
            .unwrap()
    );
    store
        .publish_conversation_reply("source-launch", "reply", body, &digest, 4)
        .await
        .unwrap();
    drop(service);
    let service = PersistentControlService::start(store.clone(), 2)
        .await
        .unwrap();
    service
        .configure_conversation_agents(vec![lead.clone(), worker])
        .unwrap();
    let proposal = service.inspect_native_delegation("reply").await.unwrap();
    assert_eq!(proposal.source_reply_id, "reply");
    assert_eq!(proposal.origin.parent_message_id, "request");
    assert_eq!(proposal.origin.author_agent_id, lead.id);
    assert_eq!(proposal.plan.assignments.len(), 1);
    assert_eq!(proposal.plan.assignments[0].agent_id, "worker");
    assert_eq!(
        proposal.plan.assignments[0].instruction,
        "Inspect the parser"
    );
    assert!(store.next_conversation_job().await.unwrap().is_none());
    assert!(store.next_room_assignment().await.unwrap().is_none());
    assert!(store.workflows().await.unwrap().is_empty());
}

/// Replies with a strict delegation decision unless the prompt carries a stored
/// assignment, in which case it reports finished work. Each launch is recorded.
fn delegating_fixture(
    root: &std::path::Path,
    lead: &str,
) -> (ConversationAgent, ConversationAgent) {
    let worker = root.join("delegating-fixture");
    std::fs::write(&worker, r#"#!/bin/sh
set -eu
printf 'launch\n' >> launches
printf '%s\n' "$@" >> prompts
case "$*" in
  *"assigned you this instruction"*) text='Worker finished the inspection';;
  *) text='{\"schema\":\"conductor.delegation.v1\",\"assignments\":[{\"agent_id\":\"worker\",\"instruction\":\"Inspect the parser\"}]}';;
esac
printf '%s\n' '{"type":"thread.started","thread_id":"fixture"}' '{"type":"turn.started"}' "{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"$text\"}}" '{"type":"turn.completed","usage":{}}'
"#).unwrap();
    std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (_, template) = fixture_agent(root);
    let mut coordinator = template;
    coordinator.id = lead.into();
    coordinator.vendor.executable.clone_from(&worker);
    coordinator.vendor.executable_sha256 =
        format!("{:x}", Sha256::digest(std::fs::read(&worker).unwrap()));
    let mut subordinate = coordinator.clone();
    subordinate.id = "worker".into();
    (coordinator, subordinate)
}

async fn drain(
    service: &PersistentControlService,
    receiver: &tokio::sync::watch::Receiver<bool>,
) -> usize {
    for round in 0..24 {
        if !service
            .dispatch_next_conversation(receiver.clone())
            .await
            .unwrap()
        {
            return round;
        }
    }
    panic!("room work did not settle: routing or delegation must not loop");
}

async fn seeded_room(root: &std::path::Path, database: &str) -> Store {
    let store = Store::open(root.join(database)).await.unwrap();
    store
        .create_conversation("room", root.to_str().unwrap(), 1)
        .await
        .unwrap();
    store
        .append_conversation_message(
            &ConversationMessage {
                sequence: 0,
                id: "shared-request".into(),
                conversation_id: "room".into(),
                sender: "operator".into(),
                agent_id: None,
                body: "Please inspect the parser".into(),
                created_at_ms: 0,
            },
            2,
        )
        .await
        .unwrap();
    store
}

fn launches(root: &std::path::Path) -> usize {
    std::fs::read_to_string(root.join("launches"))
        .unwrap()
        .lines()
        .count()
}

/// A configured two-member room after one default operator request has been
/// dispatched to rest. `keep_worker` false drops the worker's profile on restart.
async fn delegated_room(
    root: &std::path::Path,
    keep_worker: bool,
) -> (
    Store,
    PersistentControlService,
    tokio::sync::watch::Receiver<bool>,
) {
    let (lead, worker) = delegating_fixture(root, "lead");
    let store = seeded_room(root, "room.sqlite3").await;
    let service = PersistentControlService::start(store.clone(), 1)
        .await
        .unwrap();
    service
        .configure_conversation_agents(vec![lead.clone(), worker.clone()])
        .unwrap();
    service
        .configure_room("room", shared_configuration(&lead.id, &worker.id))
        .await
        .unwrap();
    drop(service);
    let service = PersistentControlService::start(store.clone(), 2)
        .await
        .unwrap();
    let profiles = if keep_worker {
        vec![lead, worker]
    } else {
        vec![lead]
    };
    service.configure_conversation_agents(profiles).unwrap();
    // The default route reaches only the coordinator; nobody selects the worker.
    service
        .route_room_request("shared-request", &[], false)
        .await
        .unwrap();
    let (cancel, receiver) = tokio::sync::watch::channel(false);
    std::mem::forget(cancel);
    drain(&service, &receiver).await;
    (store, service, receiver)
}

#[tokio::test]
async fn coordinator_delegates_to_its_subordinate_without_operator_relay() {
    let root = tempfile::tempdir().unwrap();
    let (store, _service, _receiver) = delegated_room(root.path(), true).await;
    assert_eq!(launches(root.path()), 2);
    let messages = store.conversation_messages("room", 0).await.unwrap();
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[1].agent_id.as_deref(), Some("lead"));
    assert!(
        messages[1]
            .body
            .starts_with("{\"schema\":\"conductor.delegation.v1\"")
    );
    assert_eq!(messages[2].agent_id.as_deref(), Some("worker"));
    assert_eq!(messages[2].body, "Worker finished the inspection");
    assert!(store.workflows().await.unwrap().is_empty());
}
