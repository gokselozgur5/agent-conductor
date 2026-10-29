//! Store-backed file-operation journal. This is not a native launch admission
//! or a shutdown receipt: stream termination leaves durable ownership open.
use agent_conductor_store::{EffectAudit, FileBrokerBinding, Store};
use conductor_runtime::{
    BrokerError, BrokerFileResult, BrokerFilesystem, BrokerIntent, BrokerJournal, BrokerScope,
    BrokerStreamEnd, serve_broker_stream,
};
use std::{sync::Arc, time::Duration};
use tokio::{net::UnixStream, runtime::Handle, sync::watch};
use uuid::Uuid;

/// A fresh durable registration consumed by exactly one stream server.
/// No clone, replay constructor, or automatic close-on-drop is provided.
/// The trusted caller must pin the supplied filesystem root to the binding's
/// root identity and establish OS isolation before exposing its proxy handoff.
pub struct RegisteredBrokerJournal {
    store: Store,
    binding: FileBrokerBinding,
    scope: BrokerScope,
    runtime: Handle,
}

impl RegisteredBrokerJournal {
    /// Register before exporting the private handoff to a child. A repeated
    /// registration never creates another operation path, even if still open.
    /// Cancellation after the store commit may leave an open broker; recovery
    /// must retain that uncertainty instead of constructing a replacement.
    /// # Errors
    /// Refuses invalid, stale or already registered bindings and storage failure.
    pub async fn register(store: Store, binding: FileBrokerBinding) -> Result<Self, BrokerError> {
        let runtime = Handle::try_current().map_err(|_| BrokerError::Invalid)?;
        let scope = BrokerScope {
            authority_id: binding.authority_id.clone(),
            boot_generation: binding
                .boot_generation
                .try_into()
                .map_err(|_| BrokerError::Invalid)?,
            invocation_id: binding.invocation_id.clone(),
            task_id: binding.task_id.clone(),
            attempt_id: binding.attempt_id.clone(),
            lease_epoch: binding
                .lease_epoch
                .try_into()
                .map_err(|_| BrokerError::Invalid)?,
        };
        let id = Uuid::now_v7().to_string();
        if !store
            .register_file_broker(&binding, audit(&id, &binding))
            .await
            .map_err(|_| BrokerError::Admission)?
        {
            return Err(BrokerError::Refused);
        }
        Ok(Self {
            store,
            binding,
            scope,
            runtime,
        })
    }

    /// Consume this registration to persist every intent before its filesystem
    /// effect and every result before its reply. Callbacks run only inside the
    /// transport's blocking worker, with the owning Tokio runtime kept alive.
    /// A local stream end does not close the durable broker or fence execution.
    /// # Errors
    /// Refuses invalid transport configuration; operation failures are returned
    /// as stream outcomes with the original pending database state preserved.
    pub async fn serve(
        self,
        stream: UnixStream,
        broker: BrokerFilesystem,
        cancel: watch::Receiver<bool>,
        frame_timeout: Duration,
        settle_timeout: Duration,
    ) -> Result<BrokerStreamEnd, BrokerError> {
        serve_broker_stream(
            stream,
            broker,
            Arc::new(StoreJournal(self)),
            cancel,
            frame_timeout,
            settle_timeout,
        )
        .await
    }
}

// Private so synchronous methods cannot accidentally be called by an async
// executor task. Only serve_broker_stream invokes them, on spawn_blocking.
struct StoreJournal(RegisteredBrokerJournal);
impl StoreJournal {
    fn sequence(&self, intent: &BrokerIntent) -> Result<i64, BrokerError> {
        if intent.scope != self.0.scope {
            return Err(BrokerError::Refused);
        }
        intent.sequence.try_into().map_err(|_| BrokerError::Invalid)
    }
}
impl BrokerJournal for StoreJournal {
    fn admit(&self, intent: &BrokerIntent) -> Result<(), BrokerError> {
        let sequence = self.sequence(intent)?;
        let id = Uuid::now_v7().to_string();
        match self.0.runtime.block_on(self.0.store.admit_file_operation(
            &self.0.binding,
            sequence,
            &intent.request_sha256,
            audit(&id, &self.0.binding),
        )) {
            Ok(true) => Ok(()),
            Ok(false) | Err(_) => Err(BrokerError::Admission),
        }
    }
    fn complete(
        &self,
        intent: &BrokerIntent,
        result: &BrokerFileResult,
    ) -> Result<(), BrokerError> {
        let sequence = self.sequence(intent)?;
        let id = Uuid::now_v7().to_string();
        let (BrokerFileResult::Text { sha256, .. } | BrokerFileResult::Written { sha256 }) = result;
        // Even an identical pre-existing result is not this invocation's fresh
        // durable acknowledgement. Preserve uncertainty rather than retry.
        match self.0.runtime.block_on(self.0.store.finish_file_operation(
            &self.0.binding,
            sequence,
            &intent.request_sha256,
            sha256,
            audit(&id, &self.0.binding),
        )) {
            Ok(true) => Ok(()),
            Ok(false) | Err(_) => Err(BrokerError::Uncertain),
        }
    }
}
fn audit<'a>(id: &'a str, binding: &'a FileBrokerBinding) -> EffectAudit<'a> {
    EffectAudit {
        id,
        actor_id: "file-broker",
        correlation_id: &binding.execution_token,
        now_ms: crate::clock::wall_ms(),
    }
}

impl RegisteredBrokerJournal {
    /// Serve journal requests from the trusted wrapper over a separately
    /// authenticated private stream. This is not the MCP/worker socket. The
    /// wrapper alone owns file handles; this owner persists hashes and replies
    /// only after commit. Cancellation/EOF leaves broker ownership open.
    /// # Errors
    /// Rejects malformed, reordered, replayed or stale requests, lost peers and
    /// bounded I/O/store waits. No failure authorizes a retry or execution fence.
    pub async fn serve_remote(
        self,
        stream: UnixStream,
        mut cancel: watch::Receiver<bool>,
        wait: Duration,
    ) -> Result<(), BrokerError> {
        use conductor_runtime::{BrokerJournalRequest, MAX_JOURNAL_FRAME};
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
        if wait.is_zero() || wait > Duration::from_secs(30) {
            return Err(BrokerError::Invalid);
        }
        let mut stream = BufReader::new(stream);
        let mut completed = 0;
        let mut pending = None;
        loop {
            let mut bytes = Vec::new();
            let read = async {
                (&mut stream)
                    .take(MAX_JOURNAL_FRAME as u64 + 1)
                    .read_until(b'\n', &mut bytes)
                    .await
                    .map_err(|_| BrokerError::Uncertain)?;
                Ok::<_, BrokerError>(())
            };
            tokio::select! {
                biased;
                () = journal_cancelled(&mut cancel) => return Err(BrokerError::Revoked),
                result = tokio::time::timeout(wait, read) => result.map_err(|_|BrokerError::Uncertain)??,
            }
            if bytes.is_empty() {
                return Ok(());
            }
            if bytes.last() != Some(&b'\n') {
                return Err(BrokerError::Invalid);
            }
            let request = BrokerJournalRequest::decode(&bytes)?;
            if request.scope != self.scope
                || request.sequence != completed + 1
                || match (&pending, &request.result_sha256) {
                    (None, None) => false,
                    (Some(hash), Some(_)) => hash != &request.request_sha256,
                    _ => true,
                }
            {
                return Err(BrokerError::Refused);
            }
            tokio::select! {
                biased;
                () = journal_cancelled(&mut cancel) => return Err(BrokerError::Revoked),
                result = tokio::time::timeout(wait, self.persist_remote(&request, stream.get_mut())) =>
                    result.map_err(|_|BrokerError::Uncertain)??,
            }
            if request.result_sha256.is_some() {
                completed = request.sequence;
                pending = None;
            } else {
                pending = Some(request.request_sha256);
            }
        }
    }

    async fn persist_remote(
        &self,
        request: &conductor_runtime::BrokerJournalRequest,
        stream: &mut UnixStream,
    ) -> Result<(), BrokerError> {
        use tokio::io::AsyncWriteExt;
        let id = Uuid::now_v7().to_string();
        let sequence = i64::try_from(request.sequence).map_err(|_| BrokerError::Invalid)?;
        let fresh = if let Some(result) = &request.result_sha256 {
            self.store
                .finish_file_operation(
                    &self.binding,
                    sequence,
                    &request.request_sha256,
                    result,
                    audit(&id, &self.binding),
                )
                .await
        } else {
            self.store
                .admit_file_operation(
                    &self.binding,
                    sequence,
                    &request.request_sha256,
                    audit(&id, &self.binding),
                )
                .await
        }
        .map_err(|_| BrokerError::Uncertain)?;
        if !fresh {
            return Err(BrokerError::Refused);
        }
        let ack = conductor_runtime::BrokerJournalAck {
            version: 1,
            journal_sha256: request.sha256()?,
        };
        let mut bytes = serde_json::to_vec(&ack).map_err(|_| BrokerError::Uncertain)?;
        bytes.push(b'\n');
        stream
            .write_all(&bytes)
            .await
            .map_err(|_| BrokerError::Uncertain)
    }
}
async fn journal_cancelled(cancel: &mut watch::Receiver<bool>) {
    loop {
        if *cancel.borrow() || cancel.changed().await.is_err() {
            return;
        }
    }
}
