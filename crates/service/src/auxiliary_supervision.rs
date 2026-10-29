//! Actual auxiliary transport and receipt reconciliation. Never persist launch secrets.
use crate::{Clock, PersistentControlService};
use agent_conductor_store::{
    AuxiliaryInvocation, AuxiliaryLaunchBinding, AuxiliaryPreparation, EffectAudit,
    ExecutionProcess, StoreError,
};
use conductor_runtime::{
    AuxiliaryEvent, AuxiliaryLaunch, AuxiliaryObservation, AuxiliaryReceipt, AuxiliarySession,
    AuxiliaryStopReason, ChildIdentity, HostEpoch, ProcessError, ProcessOutput, ProcessSpec,
    read_auxiliary_receipt,
};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Read},
    os::unix::fs::DirBuilderExt,
    path::Path,
};
use tokio::sync::watch;
use uuid::Uuid;

fn invalid() -> StoreError {
    StoreError::Integrity("auxiliary supervision or proof unavailable".into())
}
fn file_hash(path: &Path) -> Result<String, StoreError> {
    let file = fs::File::open(path).map_err(|_| invalid())?;
    if !file.metadata().map_err(|_| invalid())?.is_file() {
        return Err(invalid());
    }
    let mut hash = Sha256::new();
    let size = io::copy(&mut file.take(512 * 1024 * 1024 + 1), &mut hash).map_err(|_| invalid())?;
    if size > 512 * 1024 * 1024 {
        return Err(invalid());
    }
    Ok(format!("{:x}", hash.finalize()))
}
fn stored_process(process: &ChildIdentity) -> ExecutionProcess {
    ExecutionProcess {
        platform: process.host.platform.clone(),
        host_id: process.host.host_id.clone(),
        boot_id: process.host.boot_id.clone(),
        pid: process.pid,
        start_marker: process.start_marker.clone(),
    }
}
fn native_process(process: &ExecutionProcess) -> ChildIdentity {
    ChildIdentity {
        host: HostEpoch {
            platform: process.platform.clone(),
            host_id: process.host_id.clone(),
            boot_id: process.boot_id.clone(),
        },
        pid: process.pid,
        start_marker: process.start_marker.clone(),
    }
}
fn event_result(event: AuxiliaryEvent) -> Result<ProcessOutput, ProcessError> {
    match event {
        AuxiliaryEvent::Finished {
            exit_code,
            stdout,
            stderr,
        } => Ok(ProcessOutput {
            exit_code,
            stdout,
            stderr,
        }),
        AuxiliaryEvent::Stopped { reason } => Err(match reason {
            AuxiliaryStopReason::Cancelled => ProcessError::Cancelled,
            AuxiliaryStopReason::TimedOut => ProcessError::Timeout,
            AuxiliaryStopReason::OutputLimit => ProcessError::OutputLimit,
        }),
        AuxiliaryEvent::Ready { .. } => Err(ProcessError::InvalidSpec),
    }
}
impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn supervise_auxiliary(
        &self,
        input: &AuxiliaryPreparation,
        spec: &ProcessSpec,
        wrapper: &Path,
        cancellation: watch::Receiver<bool>,
    ) -> Result<Result<ProcessOutput, ProcessError>, StoreError> {
        let (authority, generation) = self.store.runtime_authority().await?;
        if generation != self.boot_generation {
            return Err(invalid());
        }
        let wrapper = wrapper.canonicalize().map_err(|_| invalid())?;
        let parent = Path::new(&input.workspace)
            .parent()
            .ok_or_else(invalid)?
            .canonicalize()
            .map_err(|_| invalid())?;
        let root = parent.join(format!("auxiliary-{}", input.invocation_id));
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .map_err(|_| invalid())?;
        let executable = spec.executable.canonicalize().map_err(|_| invalid())?;
        let launch = AuxiliaryLaunch {
            version: 1,
            invocation_id: input.invocation_id.clone(),
            authority_id: authority.ok_or_else(invalid)?,
            boot_generation: generation,
            receipt_root: root.clone(),
            executable_sha256: file_hash(&executable)?,
            executable,
            arguments: spec.arguments.clone(),
            directory: spec.directory.clone(),
            environment: spec.environment.clone(),
            timeout_ms: u64::try_from(spec.timeout.as_millis()).map_err(|_| invalid())?,
            heartbeat_timeout_ms: 3000,
            output_limit: spec.output_limit,
        };
        let binding = AuxiliaryLaunchBinding {
            launch_sha256: launch.sha256().map_err(|_| invalid())?,
            authority_id: launch.authority_id.clone(),
            receipt_root: root.to_str().ok_or_else(invalid)?.into(),
            wrapper_path: wrapper.to_str().ok_or_else(invalid)?.into(),
            wrapper_sha256: file_hash(&wrapper)?,
        };
        let id = Uuid::now_v7().to_string();
        if !self
            .store
            .bind_auxiliary_launch(input, &binding, self.auxiliary_audit(&id, input))
            .await?
        {
            return Err(invalid());
        }
        if file_hash(&wrapper)? != binding.wrapper_sha256 {
            return Err(invalid());
        }
        let session = AuxiliarySession::prepare(&wrapper, &launch)
            .await
            .map_err(|_| invalid())?;
        let process = stored_process(session.identity());
        let id = Uuid::now_v7().to_string();
        if !self
            .store
            .authorize_auxiliary_release(
                input,
                &binding,
                &process,
                self.auxiliary_audit(&id, input),
            )
            .await?
        {
            return Err(invalid());
        }
        let result = session.run(cancellation).await;
        // Even transport success cannot certify settlement without private proof.
        self.accept_auxiliary_receipt(input, &binding, &process)
            .await?;
        Ok(match result {
            Ok(event) => event_result(event),
            Err(error) => Err(ProcessError::Io(error)),
        })
    }

    fn auxiliary_audit<'a>(&self, id: &'a str, input: &'a AuxiliaryPreparation) -> EffectAudit<'a> {
        EffectAudit {
            id,
            actor_id: "conductor-runtime",
            correlation_id: &input.invocation_id,
            now_ms: self.clock.read(self.boot_generation).wall_unix_ms,
        }
    }

    async fn accept_auxiliary_receipt(
        &self,
        input: &AuxiliaryPreparation,
        binding: &AuxiliaryLaunchBinding,
        process: &ExecutionProcess,
    ) -> Result<(), StoreError> {
        if file_hash(Path::new(&binding.wrapper_path))? != binding.wrapper_sha256 {
            return Err(invalid());
        }
        let mut expected = AuxiliaryReceipt {
            version: 1,
            invocation_id: input.invocation_id.clone(),
            authority_id: binding.authority_id.clone(),
            boot_generation: u64::try_from(input.boot_generation).map_err(|_| invalid())?,
            launch_sha256: binding.launch_sha256.clone(),
            process: native_process(process),
            observation: AuxiliaryObservation::OwnedGroupSettled,
        };
        let receipt = if let Ok(value) =
            read_auxiliary_receipt(Path::new(&binding.receipt_root), &expected)
        {
            value
        } else {
            expected.observation = AuxiliaryObservation::NeverReleased;
            read_auxiliary_receipt(Path::new(&binding.receipt_root), &expected)
                .map_err(|_| invalid())?
        };
        let proof = serde_json::to_string(&receipt).map_err(|_| invalid())?;
        let id = Uuid::now_v7().to_string();
        self.store
            .settle_auxiliary_proof(
                input,
                binding,
                process,
                self.boot_generation,
                &proof,
                self.auxiliary_audit(&id, input),
            )
            .await
    }

    pub(super) async fn recover_auxiliary_receipts(&self) -> Result<usize, StoreError> {
        let mut count = 0;
        for AuxiliaryInvocation {
            preparation,
            binding,
            process,
            release_recorded,
        } in self.store.pending_auxiliary_invocations().await?
        {
            let (Some(binding), Some(process), true) = (binding, process, release_recorded) else {
                continue;
            };
            match self
                .accept_auxiliary_receipt(&preparation, &binding, &process)
                .await
            {
                Ok(()) => count += 1,
                Err(StoreError::Integrity(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(count)
    }
}
