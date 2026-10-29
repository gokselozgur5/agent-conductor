#![cfg(unix)]
use conductor_runtime::{
    BrokerFileResult, BrokerIntent, BrokerJournal, BrokerJournalAck, BrokerJournalRequest,
    BrokerScope, RemoteBrokerJournal,
};
use std::{
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::UnixStream,
    time::{Duration, Instant},
};
fn intent() -> BrokerIntent {
    BrokerIntent {
        scope: BrokerScope {
            authority_id: "a".repeat(64),
            boot_generation: 1,
            invocation_id: uuid::Uuid::now_v7().to_string(),
            task_id: "task".into(),
            attempt_id: "attempt".into(),
            lease_epoch: 1,
        },
        sequence: 1,
        request_sha256: "b".repeat(64),
    }
}
#[test]
fn exact_acknowledgement_binds_phase_scope_and_result() {
    let intent = intent();
    let (client, peer) = UnixStream::pair().unwrap();
    let journal =
        RemoteBrokerJournal::new(client, intent.scope.clone(), Duration::from_secs(2)).unwrap();
    let server = std::thread::spawn(move || {
        let mut peer = BufReader::new(peer);
        for complete in [false, true] {
            let mut bytes = Vec::new();
            peer.read_until(b'\n', &mut bytes).unwrap();
            let request = BrokerJournalRequest::decode(&bytes).unwrap();
            assert_eq!(request.result_sha256.is_some(), complete);
            let ack = BrokerJournalAck {
                version: 1,
                journal_sha256: request.sha256().unwrap(),
            };
            let mut bytes = serde_json::to_vec(&ack).unwrap();
            bytes.push(b'\n');
            peer.get_mut().write_all(&bytes).unwrap();
        }
        let mut byte = [0];
        assert_eq!(peer.read(&mut byte).unwrap(), 0);
    });
    journal.admit(&intent).unwrap();
    journal
        .complete(
            &intent,
            &BrokerFileResult::Written {
                sha256: "c".repeat(64),
            },
        )
        .unwrap();
    assert!(journal.admit(&intent).is_err());
    server.join().unwrap();
}
#[test]
fn wrong_oversized_and_missing_ack_poison_channel_without_retry() {
    for response in [
        b"{\"version\":1,\"journal_sha256\":\"wrong\"}\n".to_vec(),
        vec![b'x'; 257],
        vec![],
    ] {
        let intent = intent();
        let (client, peer) = UnixStream::pair().unwrap();
        let journal =
            RemoteBrokerJournal::new(client, intent.scope.clone(), Duration::from_millis(100))
                .unwrap();
        let server = std::thread::spawn(move || {
            peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut peer = BufReader::new(peer);
            let mut bytes = Vec::new();
            peer.read_until(b'\n', &mut bytes).unwrap();
            if response.is_empty() {
                std::thread::sleep(Duration::from_millis(200));
            } else {
                peer.get_mut().write_all(&response).unwrap();
            }
            let mut byte = [0];
            assert_eq!(peer.read(&mut byte).unwrap(), 0);
        });
        assert!(journal.admit(&intent).is_err());
        assert!(journal.admit(&intent).is_err());
        server.join().unwrap();
    }
}
#[test]
fn slow_partial_ack_cannot_reset_total_deadline() {
    let intent = intent();
    let (client, peer) = UnixStream::pair().unwrap();
    let journal =
        RemoteBrokerJournal::new(client, intent.scope.clone(), Duration::from_millis(100)).unwrap();
    let server = std::thread::spawn(move || {
        let mut peer = BufReader::new(peer);
        let mut bytes = Vec::new();
        peer.read_until(b'\n', &mut bytes).unwrap();
        for _ in 0..100 {
            if peer.get_mut().write_all(b" ").is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });
    let started = Instant::now();
    assert!(journal.admit(&intent).is_err());
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(journal.admit(&intent).is_err());
    server.join().unwrap();
}
#[test]
fn completion_before_admission_never_reaches_peer() {
    let intent = intent();
    let (client, mut peer) = UnixStream::pair().unwrap();
    let journal =
        RemoteBrokerJournal::new(client, intent.scope.clone(), Duration::from_secs(2)).unwrap();
    assert!(
        journal
            .complete(
                &intent,
                &BrokerFileResult::Written {
                    sha256: "c".repeat(64)
                }
            )
            .is_err()
    );
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).unwrap(), 0);
}
