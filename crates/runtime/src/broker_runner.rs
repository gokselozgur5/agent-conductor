//! Dedicated trusted broker mode. It grants no vendor admission or execution fence.
use crate::{
    BrokerEndpoint, BrokerError, BrokerFilesystem, BrokerScope, BrokerStreamEnd,
    BrokerStreamReason, RemoteBrokerJournal, connect_broker_endpoint, continuous_millis,
    serve_owned_broker_stream,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    sync::watch,
};

/// Private launch pipe only. Contains a bearer capability: never log or persist.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerRunnerLaunch {
    pub scope: BrokerScope,
    pub root: PathBuf,
    pub root_identity_sha256: String,
    pub endpoint_directory: PathBuf,
    pub receipt_root: PathBuf,
    pub journal_handoff: String,
    pub maximum_deadline_ms: u64,
    pub heartbeat_timeout_ms: u64,
}
impl BrokerRunnerLaunch {
    /// Hash the exact private launch for acknowledgement/release binding.
    /// # Errors
    /// Refuses invalid identities, paths, deadlines and oversized launch frames.
    pub fn sha256(&self) -> Result<String, BrokerError> {
        let now = continuous_millis().map_err(|_| BrokerError::Clock)?;
        if !self.scope.valid()
            || !self.root.is_absolute()
            || !self.receipt_root.is_absolute()
            || self.receipt_root.starts_with(&self.root)
            || !self.endpoint_directory.is_absolute()
            || self.endpoint_directory.starts_with(&self.root)
            || self.maximum_deadline_ms <= now
            || self.maximum_deadline_ms - now > 86_400_000
            || !(1..=30_000).contains(&self.heartbeat_timeout_ms)
            || self.journal_handoff.len() > 8192
        {
            return Err(BrokerError::Invalid);
        }
        let bytes = serde_json::to_vec(self).map_err(|_| BrokerError::Invalid)?;
        if bytes.len() + 1 > 32768 {
            return Err(BrokerError::Invalid);
        }
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrokerRunnerControl {
    Release {
        launch_sha256: String,
        lease_deadline_ms: u64,
    },
    Renew {
        launch_sha256: String,
        lease_deadline_ms: u64,
    },
    Cancel,
}
/// Ready contains a private proxy capability; stdout is a private controller pipe.
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrokerRunnerEvent {
    Ready {
        launch_sha256: String,
        root_identity_sha256: String,
        proxy_handoff: String,
        process: crate::ChildIdentity,
    },
    /// Local observation only. None retains uncertainty, including a pending
    /// blocking operation. Neither exit nor Some is a durable shutdown receipt.
    Stopped {
        launch_sha256: String,
        completed_sequence: Option<u64>,
    },
}
async fn frame(
    input: &mut (impl AsyncBufRead + Unpin),
    limit: usize,
) -> Result<Vec<u8>, BrokerError> {
    let mut bytes = Vec::new();
    input
        .take(limit as u64 + 1)
        .read_until(b'\n', &mut bytes)
        .await
        .map_err(|_| BrokerError::Uncertain)?;
    if bytes.len() > limit || bytes.last() != Some(&b'\n') {
        return Err(BrokerError::Invalid);
    }
    Ok(bytes)
}
async fn emit(event: BrokerRunnerEvent) -> Result<(), BrokerError> {
    let mut bytes = serde_json::to_vec(&event).map_err(|_| BrokerError::Invalid)?;
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut output = tokio::io::stdout();
        output.write_all(&bytes).await?;
        output.flush().await
    })
    .await
    .map_err(|_| BrokerError::Uncertain)?
    .map_err(|_| BrokerError::Uncertain)
}
fn now() -> Result<u64, BrokerError> {
    continuous_millis().map_err(|_| BrokerError::Clock)
}
fn valid_lease(deadline: u64, maximum: u64) -> Result<u64, BrokerError> {
    let now = now()?;
    if deadline <= now || deadline > maximum {
        return Err(BrokerError::Revoked);
    }
    Ok(now)
}
/// Serve the runner's dedicated broker mode. No model is spawned here. Only a
/// separately admitted controller can later export its proxy handoff to a vendor.
/// # Errors
/// Rejects invalid launch/root/journal binding, missing release and control loss.
/// Every return drops the only serving path; uncertain operations stay unknown.
pub async fn serve_broker_runner() -> Result<(), BrokerError> {
    let mut input = BufReader::new(tokio::io::stdin());
    let launch: BrokerRunnerLaunch = serde_json::from_slice(
        &tokio::time::timeout(Duration::from_secs(5), frame(&mut input, 32768))
            .await
            .map_err(|_| BrokerError::Uncertain)??,
    )
    .map_err(|_| BrokerError::Invalid)?;
    let launch_hash = launch.sha256()?;
    let broker = BrokerFilesystem::new(
        &launch.root,
        launch.scope.clone(),
        launch.maximum_deadline_ms,
    )?;
    if broker.root_identity_sha256()? != launch.root_identity_sha256 {
        return Err(BrokerError::Refused);
    }
    let evidence = crate::BrokerReceiptDirectory::open(&launch.receipt_root)?;
    evidence.claim(&launch.scope, &launch_hash)?;
    let process = crate::ChildIdentity::observe(std::process::id())
        .await
        .map_err(|_| BrokerError::Refused)?;
    let mut receipt = crate::BrokerShutdownReceipt {
        version: 1,
        scope: launch.scope.clone(),
        launch_sha256: launch_hash.clone(),
        root_identity_sha256: launch.root_identity_sha256.clone(),
        process: process.clone(),
        observation: crate::BrokerShutdownObservation::NeverReleased,
    };
    let lease = broker.lease_control();
    let prepared = prepare_broker(&launch, &launch_hash, &process, &lease, &mut input).await;
    let (endpoint, journal, started, lease_deadline_ms) = match prepared {
        Ok(value) => value,
        Err(error) => {
            drop(broker);
            evidence.publish(&receipt)?;
            return Err(error);
        }
    };
    let (cancel, rx) = watch::channel(false);
    let end = {
        let worker = broker_worker(endpoint, broker, journal, rx);
        tokio::pin!(worker);
        supervise(
            &mut input,
            &launch,
            &launch_hash,
            (started, lease_deadline_ms),
            &cancel,
            worker.as_mut(),
            &lease,
        )
        .await?
    };
    if let Some(completed_sequence) = end.completed_sequence {
        receipt.observation = crate::BrokerShutdownObservation::Revoked { completed_sequence };
        evidence.publish(&receipt)?;
    }
    emit(BrokerRunnerEvent::Stopped {
        launch_sha256: launch_hash,
        completed_sequence: end.completed_sequence,
    })
    .await
}
async fn prepare_broker(
    launch: &BrokerRunnerLaunch,
    launch_hash: &str,
    process: &crate::ChildIdentity,
    lease: &crate::BrokerLeaseDeadline,
    input: &mut (impl AsyncBufRead + Unpin),
) -> Result<(BrokerEndpoint, RemoteBrokerJournal, u64, u64), BrokerError> {
    let (stream, scope) = connect_broker_endpoint(&launch.journal_handoff, Duration::from_secs(5))
        .await
        .map_err(|_| BrokerError::Refused)?;
    if scope != launch.scope {
        return Err(BrokerError::Refused);
    }
    let journal = RemoteBrokerJournal::new(
        stream.into_std().map_err(|_| BrokerError::Uncertain)?,
        scope,
        Duration::from_secs(5),
    )?;
    let endpoint = BrokerEndpoint::bind(&launch.endpoint_directory, launch.scope.clone())
        .map_err(|_| BrokerError::Refused)?;
    let handoff = endpoint
        .environment_value()
        .map_err(|_| BrokerError::Refused)?;
    emit(BrokerRunnerEvent::Ready {
        launch_sha256: launch_hash.to_owned(),
        root_identity_sha256: launch.root_identity_sha256.clone(),
        proxy_handoff: handoff,
        process: process.clone(),
    })
    .await?;
    let control: BrokerRunnerControl = serde_json::from_slice(
        &tokio::time::timeout(Duration::from_secs(5), frame(input, 1024))
            .await
            .map_err(|_| BrokerError::Uncertain)??,
    )
    .map_err(|_| BrokerError::Invalid)?;
    let BrokerRunnerControl::Release {
        launch_sha256,
        lease_deadline_ms,
    } = control
    else {
        return Err(BrokerError::Refused);
    };
    if launch_sha256 != launch_hash {
        return Err(BrokerError::Refused);
    }
    let started = valid_lease(lease_deadline_ms, launch.maximum_deadline_ms)?;
    lease.renew(lease_deadline_ms.min(started.saturating_add(launch.heartbeat_timeout_ms)))?;
    Ok((endpoint, journal, started, lease_deadline_ms))
}
async fn broker_worker(
    endpoint: BrokerEndpoint,
    broker: BrokerFilesystem,
    journal: RemoteBrokerJournal,
    mut cancel: watch::Receiver<bool>,
) -> Result<BrokerStreamEnd, BrokerError> {
    let stream = tokio::select! {
        biased;
        _=cancel.changed()=>return Ok(BrokerStreamEnd {reason:BrokerStreamReason::Cancelled,completed_sequence:broker.revoke().ok()}),
        result=endpoint.accept(Duration::from_secs(30))=>match result {
            Ok(stream)=>stream,
            Err(_)=>return Ok(BrokerStreamEnd {reason:BrokerStreamReason::TransportFailed,completed_sequence:broker.revoke().ok()}),
        }
    };
    serve_owned_broker_stream(
        stream,
        broker,
        Arc::new(journal),
        cancel,
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .await
}
async fn supervise<F>(
    input: &mut (impl AsyncBufRead + Unpin),
    launch: &BrokerRunnerLaunch,
    hash: &str,
    lease_state: (u64, u64),
    cancel: &watch::Sender<bool>,
    mut worker: std::pin::Pin<&mut F>,
    lease: &crate::BrokerLeaseDeadline,
) -> Result<BrokerStreamEnd, BrokerError>
where
    F: std::future::Future<Output = Result<BrokerStreamEnd, BrokerError>>,
{
    let (mut renewed, mut deadline) = lease_state;
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next = Box::pin(frame(input, 1024));
    loop {
        tokio::select! {
            result=&mut worker=>return result,
            _=tick.tick()=>match now() {
                Ok(now) if now>=renewed && now<deadline && now<launch.maximum_deadline_ms && now-renewed<launch.heartbeat_timeout_ms=>{},
                _=>break,
            },
            result=&mut next=> {
                match result.and_then(|bytes|serde_json::from_slice::<BrokerRunnerControl>(&bytes).map_err(|_|BrokerError::Invalid)) {
                    Ok(BrokerRunnerControl::Renew {launch_sha256,lease_deadline_ms}) if launch_sha256==hash=>{
                        let Ok(current)=valid_lease(lease_deadline_ms,launch.maximum_deadline_ms) else {break;};
                        if current<renewed || current>=deadline || current-renewed>=launch.heartbeat_timeout_ms {break;}
                        if lease.renew(lease_deadline_ms.min(current.saturating_add(launch.heartbeat_timeout_ms))).is_err() {break;}
                        deadline=lease_deadline_ms;renewed=current;
                    },
                    _=>break,
                }
                drop(next);next=Box::pin(frame(input,1024));
            }
        }
    }
    lease.revoke();
    let _ = cancel.send(true);
    worker.await
}
