#![cfg(unix)]
use conductor_runtime::{AccountChannelError, AccountChannelListener, connect_account_channel};
use std::{fs, os::unix::fs::PermissionsExt, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const WAIT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn authenticated_packet_channel_preserves_bidirectional_bytes_and_unlinks_listener() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("account.sock");
    let listener =
        AccountChannelListener::bind(&path, nix::unistd::geteuid().as_raw(), "a".repeat(64))
            .unwrap();
    let address = listener.address().clone();
    let (server, client) = tokio::join!(
        listener.accept(WAIT),
        connect_account_channel(&address, WAIT)
    );
    let (mut server, mut client) = (server.unwrap(), client.unwrap());
    assert!(!path.exists());
    client.write_all(b"packet\0payload").await.unwrap();
    let mut bytes = [0; 14];
    server.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"packet\0payload");
    server.write_all(b"ready").await.unwrap();
    let mut response = [0; 5];
    client.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"ready");
    drop(client);
    assert_eq!(server.read(&mut bytes).await.unwrap(), 0);
}

#[tokio::test]
async fn wrong_worker_uid_receives_no_packet_or_payload() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("peer.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let address = conductor_runtime::AccountChannelAddress {
        path,
        worker_uid: nix::unistd::geteuid().as_raw().checked_add(1).unwrap(),
        coordinator_uid: nix::unistd::geteuid().as_raw(),
        packet_sha256: "a".repeat(64),
    };
    let client = connect_account_channel(&address, WAIT);
    let server = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
    };
    let (client, ()) = tokio::join!(client, server);
    assert!(matches!(client, Err(AccountChannelError::Refused)));
}

#[tokio::test]
async fn wrong_coordinator_uid_and_packet_cannot_get_an_admitted_stream() {
    let root = tempfile::tempdir().unwrap();
    let uid = nix::unistd::geteuid().as_raw();
    for wrong_uid in [false, true] {
        let listener = AccountChannelListener::bind(
            &root.path().join("refuse.sock"),
            if wrong_uid {
                uid.checked_add(1).unwrap()
            } else {
                uid
            },
            "a".repeat(64),
        )
        .unwrap();
        let mut address = listener.address().clone();
        address.coordinator_uid = uid;
        if !wrong_uid {
            address.packet_sha256 = "b".repeat(64);
        }
        let (server, client) = tokio::join!(
            listener.accept(WAIT),
            connect_account_channel(&address, WAIT)
        );
        assert!(matches!(server, Err(AccountChannelError::Refused)));
        assert!(client.is_err());
    }
}

#[tokio::test]
async fn timeout_closes_admission_and_existing_paths_are_preserved() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("timeout.sock");
    let uid = nix::unistd::geteuid().as_raw();
    let listener = AccountChannelListener::bind(&path, uid, "a".repeat(64)).unwrap();
    assert!(AccountChannelListener::bind(&path, uid, "a".repeat(64)).is_err());
    assert!(listener.accept(Duration::from_millis(10)).await.is_err());
    assert!(!path.exists());
    let listener = AccountChannelListener::bind(&path, uid, "a".repeat(64)).unwrap();
    fs::rename(&path, root.path().join("retained.sock")).unwrap();
    fs::write(&path, b"replacement").unwrap();
    drop(listener);
    assert_eq!(fs::read(&path).unwrap(), b"replacement");
}

#[tokio::test]
async fn shared_writable_parent_and_invalid_digest_are_refused() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("invalid.sock");
    let uid = nix::unistd::geteuid().as_raw();
    assert!(AccountChannelListener::bind(&path, uid, "A".repeat(64)).is_err());
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o777)).unwrap();
    assert!(AccountChannelListener::bind(&path, uid, "a".repeat(64)).is_err());
    assert!(!path.exists());
}

#[tokio::test]
async fn stalled_partial_handshake_consumes_admission_and_expires() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("stalled.sock");
    let listener =
        AccountChannelListener::bind(&path, nix::unistd::geteuid().as_raw(), "a".repeat(64))
            .unwrap();
    let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();
    stream.write_all(b"ACCT1").await.unwrap();
    assert!(listener.accept(Duration::from_millis(10)).await.is_err());
    assert!(!path.exists());
    assert!(tokio::net::UnixStream::connect(&path).await.is_err());
}

#[tokio::test]
async fn pinned_snapshot_crosses_authenticated_channel_without_sharing_destination() {
    use conductor_runtime::{export_snapshot, import_snapshot, snapshot_paths};
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("answer"), b"42\n").unwrap();
    let snapshot = root.path().join("snapshot");
    let manifest = snapshot_paths(&source, &snapshot, &["answer".into()]).unwrap();
    let digest = manifest.sha256().unwrap();
    let listener = AccountChannelListener::bind(
        &root.path().join("transfer.sock"),
        nix::unistd::geteuid().as_raw(),
        "a".repeat(64),
    )
    .unwrap();
    let address = listener.address().clone();
    let (server, client) = tokio::join!(
        listener.accept(WAIT),
        connect_account_channel(&address, WAIT)
    );
    let mut server = server.unwrap().into_std().unwrap();
    let mut client = client.unwrap().into_std().unwrap();
    server.set_nonblocking(false).unwrap();
    client.set_nonblocking(false).unwrap();
    server.set_write_timeout(Some(WAIT)).unwrap();
    client.set_read_timeout(Some(WAIT)).unwrap();
    let target = root.path().join("received");
    let expected = digest.clone();
    let export_task = tokio::task::spawn_blocking(move || {
        export_snapshot(&snapshot.join("tree"), &manifest, &digest, &mut server).unwrap();
        server.shutdown(std::net::Shutdown::Write).unwrap();
        manifest
    });
    let import_task = tokio::task::spawn_blocking(move || {
        let received = import_snapshot(&mut client, &target, &expected).unwrap();
        assert_eq!(fs::read(target.join("tree/answer")).unwrap(), b"42\n");
        received
    });
    let (sent, received) = tokio::join!(export_task, import_task);
    assert_eq!(sent.unwrap(), received.unwrap());
    assert_eq!(fs::read(source.join("answer")).unwrap(), b"42\n");
}
