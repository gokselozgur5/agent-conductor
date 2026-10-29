//! Private, already-connected proxy transport. This does not authenticate a
//! named socket or grant filesystem access to the peer. Only the host owns it.
use crate::{
    BrokerError, BrokerFileResult, BrokerFilesystem, BrokerIntent, BrokerRequest, MAX_BROKER_FRAME,
};
use serde::Serialize;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::watch,
};

const MAX_REPLY: usize = 2 * 1024 * 1024;

/// Trusted synchronous persistence ports; false admission/replay must become
/// an error. Implementations must not retain filesystem write capability.
pub trait BrokerJournal: Send + Sync + 'static {
    /// # Errors
    /// Refuses stale, duplicate, uncommitted or otherwise unauthorized intent.
    fn admit(&self, intent: &BrokerIntent) -> Result<(), BrokerError>;
    /// # Errors
    /// Refuses any result not durably recorded against its exact admitted intent.
    fn complete(&self, intent: &BrokerIntent, result: &BrokerFileResult)
    -> Result<(), BrokerError>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerStreamReason {
    PeerClosed,
    Cancelled,
    TimedOut,
    InvalidFrame,
    TransportFailed,
    OperationFailed,
}

/// A local observation, not a persisted shutdown receipt or execution fence.
#[derive(Debug, PartialEq, Eq)]
pub struct BrokerStreamEnd {
    pub reason: BrokerStreamReason,
    /// None means the write/persistence boundary has not been proved quiescent.
    pub completed_sequence: Option<u64>,
}
#[derive(Serialize)]
struct Reply<'a> {
    version: u16,
    sequence: u64,
    result: &'a BrokerFileResult,
}

async fn stop(cancel: &mut watch::Receiver<bool>) {
    loop {
        if *cancel.borrow() {
            return;
        }
        if cancel.changed().await.is_err() {
            return;
        }
    }
}
async fn frame(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> Result<Option<Vec<u8>>, BrokerStreamReason> {
    let mut bytes = Vec::new();
    reader
        .take(MAX_BROKER_FRAME as u64 + 1)
        .read_until(b'\n', &mut bytes)
        .await
        .map_err(|_| BrokerStreamReason::TransportFailed)?;
    if bytes.is_empty() {
        return Ok(None);
    }
    if bytes.len() > MAX_BROKER_FRAME || bytes.last() != Some(&b'\n') {
        return Err(BrokerStreamReason::InvalidFrame);
    }
    Ok(Some(bytes))
}
fn end(broker: &BrokerFilesystem, reason: BrokerStreamReason) -> BrokerStreamEnd {
    // The server owns the sole operation path and calls this only with no
    // outstanding operation. A failed/poisoned gate remains uncertain.
    BrokerStreamEnd {
        reason,
        completed_sequence: broker.revoke().ok(),
    }
}

struct BrokerOwner(Arc<BrokerFilesystem>);
impl Drop for BrokerOwner {
    fn drop(&mut self) {
        self.0.request_revocation();
    }
}

/// Serve one host-provided private stream with bounded framing and no replay.
/// The stream and broker are consumed, so peers never receive the file handle.
/// Admission and result persistence execute on one blocking worker per request.
/// Cancellation closes admission before waiting for that worker. If the bounded
/// wait expires, the worker can still be running: the return explicitly retains
/// uncertainty, and the host must retain ownership and prevent continuation.
///
/// This primitive does not construct the MCP proxy, authenticate socket paths,
/// persist a shutdown receipt, or establish native adapter isolation.
/// # Errors
/// Invalid timeout configuration refuses serving and revokes the supplied broker.
pub async fn serve_broker_stream(
    stream: UnixStream,
    broker: BrokerFilesystem,
    journal: Arc<dyn BrokerJournal>,
    mut cancel: watch::Receiver<bool>,
    frame_timeout: Duration,
    settle_timeout: Duration,
) -> Result<BrokerStreamEnd, BrokerError> {
    if frame_timeout.is_zero()
        || frame_timeout > Duration::from_secs(30)
        || settle_timeout.is_zero()
        || settle_timeout > Duration::from_secs(30)
    {
        broker.request_revocation();
        return Err(BrokerError::Invalid);
    }
    let owner = BrokerOwner(Arc::new(broker));
    let broker = &owner.0;
    let (input, mut output) = stream.into_split();
    let mut reader = BufReader::new(input);
    loop {
        let bytes = tokio::select! {
            biased;
            ()=stop(&mut cancel)=>return Ok(end(broker,BrokerStreamReason::Cancelled)),
            value=tokio::time::timeout(frame_timeout,frame(&mut reader))=>match value {
                Err(_)=>return Ok(end(broker,BrokerStreamReason::TimedOut)),
                Ok(Err(reason))=>return Ok(end(broker,reason)),
                Ok(Ok(None))=>return Ok(end(broker,BrokerStreamReason::PeerClosed)),
                Ok(Ok(Some(bytes)))=>bytes,
            }
        };
        let Ok(request) = BrokerRequest::decode(&bytes) else {
            return Ok(end(broker, BrokerStreamReason::InvalidFrame));
        };
        let sequence = request.sequence;
        let operation_broker = Arc::clone(broker);
        let sink = Arc::clone(&journal);
        let mut operation = tokio::task::spawn_blocking(move || {
            operation_broker.execute_recorded(
                &request,
                |i| sink.admit(i),
                |i, r| sink.complete(i, r),
            )
        });
        let result = tokio::select! {
            biased;
            ()=stop(&mut cancel)=> {
                broker.request_revocation();
                if tokio::time::timeout(settle_timeout,&mut operation).await.is_err() {
                    return Ok(BrokerStreamEnd{reason:BrokerStreamReason::Cancelled,completed_sequence:None});
                }
                return Ok(end(broker,BrokerStreamReason::Cancelled));
            }
            value=tokio::time::timeout(frame_timeout,&mut operation)=>if let Ok(value)=value {
                value
            } else {
                    broker.request_revocation();
                    if tokio::time::timeout(settle_timeout,&mut operation).await.is_err() {
                        return Ok(BrokerStreamEnd{reason:BrokerStreamReason::TimedOut,completed_sequence:None});
                    }
                    return Ok(end(broker,BrokerStreamReason::TimedOut));
            }
        };
        let Ok(Ok(result)) = result else {
            return Ok(end(broker, BrokerStreamReason::OperationFailed));
        };
        let Ok(mut bytes) = serde_json::to_vec(&Reply {
            version: 1,
            sequence,
            result: &result,
        }) else {
            return Ok(end(broker, BrokerStreamReason::TransportFailed));
        };
        bytes.push(b'\n');
        if bytes.len() > MAX_REPLY {
            return Ok(end(broker, BrokerStreamReason::TransportFailed));
        }
        tokio::select! {
            biased;
            ()=stop(&mut cancel)=>return Ok(end(broker,BrokerStreamReason::Cancelled)),
            value=tokio::time::timeout(frame_timeout,output.write_all(&bytes))=>match value {
                Ok(Ok(()))=>{},
                Ok(Err(_))=>return Ok(end(broker,BrokerStreamReason::TransportFailed)),
                Err(_)=>return Ok(end(broker,BrokerStreamReason::TimedOut)),
            }
        }
    }
}
