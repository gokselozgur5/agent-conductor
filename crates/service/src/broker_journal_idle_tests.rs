use super::*;
use conductor_runtime::BrokerError;

#[tokio::test]
async fn journal_idle_before_admission_does_not_consume_frame_budget() {
    let (_root, store, binding, _db) = fixture().await;
    let journal = RegisteredBrokerJournal::register(store.clone(), binding.clone())
        .await
        .unwrap();
    let (host, peer) = tokio::net::UnixStream::pair().unwrap();
    let (_cancel, rx) = tokio::sync::watch::channel(false);
    let server = tokio::spawn(journal.serve_remote_owned(host, rx, Duration::from_millis(250)));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !server.is_finished(),
        "Idle ownership exhausted a frame deadline"
    );
    let mut peer = BufReader::new(peer);
    let mut request = wire_request(&binding);
    for result in [None, Some("e".repeat(64))] {
        request.result_sha256 = result;
        let mut bytes = serde_json::to_vec(&request).unwrap();
        bytes.push(b'\n');
        peer.write_all(&bytes).await.unwrap();
        let mut ack = String::new();
        tokio::time::timeout(Duration::from_secs(2), peer.read_line(&mut ack))
            .await
            .unwrap()
            .unwrap();
        let ack: conductor_runtime::BrokerJournalAck = serde_json::from_str(&ack).unwrap();
        assert_eq!(ack.journal_sha256, request.sha256().unwrap());
    }
    drop(peer);
    assert!(server.await.unwrap().is_ok());
    assert_progress(&store, &binding, 1, 1).await;
    assert!(!store.file_broker_status(&binding).await.unwrap().closed);
}

#[tokio::test]
async fn journal_partial_frame_and_pending_result_keep_deadlines() {
    for pending in [false, true] {
        let (_root, store, binding, _db) = fixture().await;
        let journal = RegisteredBrokerJournal::register(store.clone(), binding.clone())
            .await
            .unwrap();
        let (host, peer) = tokio::net::UnixStream::pair().unwrap();
        let (_cancel, rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(journal.serve_remote_owned(host, rx, Duration::from_millis(250)));
        let mut peer = BufReader::new(peer);
        if pending {
            let mut bytes = serde_json::to_vec(&wire_request(&binding)).unwrap();
            bytes.push(b'\n');
            peer.write_all(&bytes).await.unwrap();
            let mut ack = String::new();
            peer.read_line(&mut ack).await.unwrap();
            assert!(!ack.is_empty());
        } else {
            peer.write_all(b"{").await.unwrap();
        }
        let result = tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(BrokerError::Uncertain)));
        assert_progress(&store, &binding, i64::from(pending), 0).await;
        assert!(!store.file_broker_status(&binding).await.unwrap().closed);
    }
}

#[tokio::test]
async fn journal_idle_remains_cancellable_after_frame_budget() {
    let (_root, store, binding, _db) = fixture().await;
    let journal = RegisteredBrokerJournal::register(store.clone(), binding.clone())
        .await
        .unwrap();
    let (host, _peer) = tokio::net::UnixStream::pair().unwrap();
    let (cancel, rx) = tokio::sync::watch::channel(false);
    let server = tokio::spawn(journal.serve_remote_owned(host, rx, Duration::from_millis(250)));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!server.is_finished());
    cancel.send(true).unwrap();
    assert!(matches!(server.await.unwrap(), Err(BrokerError::Revoked)));
    assert_progress(&store, &binding, 0, 0).await;
}

#[tokio::test]
async fn legacy_journal_idle_still_has_a_whole_read_deadline() {
    let (_root, store, binding, _db) = fixture().await;
    let journal = RegisteredBrokerJournal::register(store.clone(), binding.clone())
        .await
        .unwrap();
    let (host, _peer) = tokio::net::UnixStream::pair().unwrap();
    let (_cancel, rx) = tokio::sync::watch::channel(false);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        journal.serve_remote(host, rx, Duration::from_millis(100)),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(BrokerError::Uncertain)));
    assert_progress(&store, &binding, 0, 0).await;
}
