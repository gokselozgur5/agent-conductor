#![cfg(unix)]
use conductor_runtime::{
    AccountChannelListener, AccountConversationConnection, AccountConversationError,
    AccountConversationEvent, AccountConversationRequest, AdapterKind, ExecutionAccount, VendorPin,
    serve_account_conversation,
};
use sha2::{Digest, Sha256};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, BufReader};

fn request(root: &std::path::Path, slow: bool) -> AccountConversationRequest {
    let executable = root.join("fixture");
    let pause = if slow { "/bin/sleep 30\n" } else { "" };
    let script = format!(
        r#"#!/bin/sh
set -eu
printf 'launch\n' >> launches
printf '%s' "$HOME" > observed-home
/usr/bin/id -u > observed-uid
printf '%s\n' "$@" > observed-arguments
{pause}printf '%s\n' '{{"type":"thread.started","thread_id":"fixture"}}' '{{"type":"turn.started"}}' '{{"type":"item.completed","item":{{"type":"agent_message","text":"Account fixture reply"}}}}' '{{"type":"turn.completed","usage":{{}}}}'
"#
    );
    fs::write(&executable, &script).unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
    let user = nix::unistd::User::from_uid(nix::unistd::getuid())
        .unwrap()
        .unwrap();
    AccountConversationRequest {
        version: 1,
        launch_id: "chat-launch".into(),
        account: ExecutionAccount::resolve(&user.name).unwrap(),
        vendor: VendorPin {
            kind: AdapterKind::Codex,
            version: "fixture".into(),
            executable,
            executable_sha256: format!("{:x}", Sha256::digest(script.as_bytes())),
            conformance_sha256: "a".repeat(64),
        },
        directory: root.into(),
        path: "/usr/bin:/bin".into(),
        prompt: "Reply to this controlled conversation".into(),
        timeout_ms: 10_000,
    }
}

fn listener(
    root: &std::path::Path,
    request: &AccountConversationRequest,
) -> AccountChannelListener {
    AccountChannelListener::bind(
        &root.join("chat.sock"),
        nix::unistd::geteuid().as_raw(),
        request.sha256().unwrap(),
    )
    .unwrap()
}

async fn receive(listener: AccountChannelListener) -> Result<(), AccountConversationError> {
    let digest = listener.address().packet_sha256.clone();
    let stream = listener.accept(Duration::from_secs(3)).await.unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    let request = serde_json::from_str(&line).unwrap();
    assert!(
        reader.buffer().is_empty(),
        "client must wait for Ready before Release"
    );
    serve_account_conversation(reader.into_inner(), request, &digest).await
}

#[tokio::test]
async fn account_conversation_waits_for_durable_binding_and_uses_own_account() {
    let root = tempfile::tempdir().unwrap();
    let request = request(root.path(), false);
    let listener = listener(root.path(), &request);
    let address = listener.address().clone();
    let recorded = Arc::new(AtomicBool::new(false));
    let receipt = Arc::clone(&recorded);
    let client = async {
        let mut connection =
            AccountConversationConnection::connect(&address, &request, |binding| {
                assert_eq!(binding.launch_id, request.launch_id);
                assert_eq!(binding.request_sha256, request.sha256().unwrap());
                assert_eq!(binding.worker_uid, request.account.uid);
                assert_eq!(binding.coordinator_uid, nix::unistd::geteuid().as_raw());
                assert!(!root.path().join("launches").exists());
                receipt.store(true, Ordering::SeqCst);
                async { Ok(()) }
            })
            .await
            .unwrap();
        let AccountConversationEvent::Finished { text, .. } = connection.event().await.unwrap()
        else {
            panic!("reply missing");
        };
        assert_eq!(text, "Account fixture reply");
    };
    let (server, ()) = tokio::join!(receive(listener), client);
    server.unwrap();
    assert!(recorded.load(Ordering::SeqCst));
    assert_eq!(
        fs::read_to_string(root.path().join("launches")).unwrap(),
        "launch\n"
    );
    assert_eq!(
        fs::read_to_string(root.path().join("observed-home")).unwrap(),
        request.account.home.to_string_lossy()
    );
    assert_eq!(
        fs::read_to_string(root.path().join("observed-uid"))
            .unwrap()
            .trim(),
        request.account.uid.to_string()
    );
    let args = fs::read_to_string(root.path().join("observed-arguments")).unwrap();
    assert!(args.starts_with("--yolo\nexec\n--json\n"));
    assert!(args.contains(&request.prompt));
}

#[tokio::test]
async fn failed_durable_ack_never_releases_the_native_process() {
    let root = tempfile::tempdir().unwrap();
    let request = request(root.path(), false);
    let listener = listener(root.path(), &request);
    let address = listener.address().clone();
    let (server, client) = tokio::join!(
        receive(listener),
        AccountConversationConnection::connect(&address, &request, |_| async {
            Err(AccountConversationError::Persistence)
        })
    );
    assert!(matches!(client, Err(AccountConversationError::Persistence)));
    assert!(server.is_err());
    assert!(!root.path().join("launches").exists());
}

#[tokio::test]
async fn stale_worker_home_is_refused_before_ready_or_native_launch() {
    let root = tempfile::tempdir().unwrap();
    let mut request = request(root.path(), false);
    request.account.home.push("not-the-selected-home");
    let listener = listener(root.path(), &request);
    let address = listener.address().clone();
    let (server, client) = tokio::join!(
        receive(listener),
        AccountConversationConnection::connect(&address, &request, |_| async {
            panic!("unvalidated receiver sent Ready")
        })
    );
    assert!(matches!(server, Err(AccountConversationError::Refused)));
    assert!(client.is_err());
    assert!(!root.path().join("launches").exists());
}

async fn wait_for_launch(path: &std::path::Path) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !path.join("launches").exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancellation_waits_for_supervision_and_reports_uncertainty() {
    let root = tempfile::tempdir().unwrap();
    let request = request(root.path(), true);
    let listener = listener(root.path(), &request);
    let address = listener.address().clone();
    let client = async {
        let mut connection =
            AccountConversationConnection::connect(&address, &request, |_| async { Ok(()) })
                .await
                .unwrap();
        wait_for_launch(root.path()).await;
        connection.cancel().await.unwrap();
        assert!(matches!(
            connection.event().await.unwrap(),
            AccountConversationEvent::Unknown { .. }
        ));
    };
    let (server, ()) = tokio::join!(receive(listener), client);
    server.unwrap();
    assert_eq!(
        fs::read_to_string(root.path().join("launches")).unwrap(),
        "launch\n"
    );
}

#[tokio::test]
async fn disconnected_controller_does_not_trigger_a_reconnect_or_second_launch() {
    let root = tempfile::tempdir().unwrap();
    let request = request(root.path(), true);
    let listener = listener(root.path(), &request);
    let address = listener.address().clone();
    let client = async {
        let connection =
            AccountConversationConnection::connect(&address, &request, |_| async { Ok(()) })
                .await
                .unwrap();
        wait_for_launch(root.path()).await;
        drop(connection);
    };
    let (server, ()) = tokio::join!(receive(listener), client);
    assert!(server.is_err());
    assert_eq!(
        fs::read_to_string(root.path().join("launches")).unwrap(),
        "launch\n"
    );
}

#[tokio::test]
async fn changed_digest_and_worker_binding_are_refused_before_connection() {
    let root = tempfile::tempdir().unwrap();
    let request = request(root.path(), false);
    let listener = listener(root.path(), &request);
    let original = listener.address().clone();
    for wrong_uid in [false, true] {
        let mut address = original.clone();
        if wrong_uid {
            address.worker_uid = address.worker_uid.checked_add(1).unwrap();
        } else {
            address.packet_sha256 = "b".repeat(64);
        }
        assert!(matches!(
            AccountConversationConnection::connect(&address, &request, |_| async {
                panic!("invalid binding recorded")
            })
            .await,
            Err(AccountConversationError::Refused)
        ));
    }
    assert!(!root.path().join("launches").exists());
}

async fn protocol_peer(listener: AccountChannelListener, bytes: Vec<u8>, after_release: bool) {
    use tokio::io::AsyncWriteExt;
    let address = listener.address().clone();
    let stream = listener.accept(Duration::from_secs(3)).await.unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    let request: AccountConversationRequest = serde_json::from_str(&line).unwrap();
    if after_release {
        let binding = conductor_runtime::AccountConversationReady {
            launch_id: request.launch_id,
            request_sha256: address.packet_sha256,
            worker_uid: address.worker_uid,
            coordinator_uid: address.coordinator_uid,
        };
        let mut ready = serde_json::to_vec(&AccountConversationEvent::Ready { binding }).unwrap();
        ready.push(b'\n');
        reader.get_mut().write_all(&ready).await.unwrap();
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        assert!(line.contains("release"));
    }
    // Oversized payload peers may be disconnected before completing their write.
    let _ = reader.get_mut().write_all(&bytes).await;
}

#[tokio::test]
async fn invalid_binding_oversized_and_truncated_ready_never_reach_ack() {
    for case in 0..3 {
        let root = tempfile::tempdir().unwrap();
        let request = request(root.path(), false);
        let listener = listener(root.path(), &request);
        let address = listener.address().clone();
        let bytes = match case {
            0 => {
                let binding = conductor_runtime::AccountConversationReady {
                    launch_id: request.launch_id.clone(),
                    request_sha256: "b".repeat(64),
                    worker_uid: address.worker_uid,
                    coordinator_uid: address.coordinator_uid,
                };
                let mut value =
                    serde_json::to_vec(&AccountConversationEvent::Ready { binding }).unwrap();
                value.push(b'\n');
                value
            }
            1 => vec![b'x'; 512 * 1024 + 1],
            _ => b"{\"type\":\"ready\"".to_vec(),
        };
        let ((), client) = tokio::join!(
            protocol_peer(listener, bytes, false),
            AccountConversationConnection::connect(&address, &request, |_| async {
                panic!("invalid frame acknowledged")
            }),
        );
        assert!(client.is_err());
        assert!(!root.path().join("launches").exists());
    }
}

#[tokio::test]
async fn repeated_ready_and_terminal_frames_are_protocol_errors() {
    for repeat_ready in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let request = request(root.path(), false);
        let listener = listener(root.path(), &request);
        let address = listener.address().clone();
        let binding = conductor_runtime::AccountConversationReady {
            launch_id: request.launch_id.clone(),
            request_sha256: request.sha256().unwrap(),
            worker_uid: address.worker_uid,
            coordinator_uid: address.coordinator_uid,
        };
        let event = if repeat_ready {
            AccountConversationEvent::Ready { binding }
        } else {
            AccountConversationEvent::Finished {
                binding,
                text: "Peer reply".into(),
                output_sha256: "a".repeat(64),
            }
        };
        let mut frame = serde_json::to_vec(&event).unwrap();
        frame.push(b'\n');
        let bytes = [frame.clone(), frame].concat();
        let client = async {
            let mut connection =
                AccountConversationConnection::connect(&address, &request, |_| async { Ok(()) })
                    .await
                    .unwrap();
            if !repeat_ready {
                assert!(matches!(
                    connection.event().await.unwrap(),
                    AccountConversationEvent::Finished { .. }
                ));
            }
            assert!(matches!(
                connection.event().await,
                Err(AccountConversationError::Protocol)
            ));
            assert!(matches!(
                connection.event().await,
                Err(AccountConversationError::Protocol)
            ));
            assert!(connection.cancel().await.is_err());
        };
        let ((), ()) = tokio::join!(protocol_peer(listener, bytes, true), client);
    }
}
