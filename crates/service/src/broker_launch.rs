//! Prepare an approved native broker and persist its actual Ready identity.
use crate::{broker_prelaunch::approved_hash, record_broker_launch};
use agent_conductor_store::{
    EffectAudit, ExecutionProcess, ExecutionUpdate, FileBrokerBinding, Store,
};
use conductor_runtime::{BrokerEndpoint, BrokerError, BrokerRunnerLaunch, PreparedBrokerRunner};
use std::{path::Path, time::Duration};
use tokio::net::UnixStream;
use uuid::Uuid;

/// Returned before journal registration or Release; the caller owns both handles.
pub struct AcknowledgedBroker {
    pub runner: PreparedBrokerRunner,
    pub journal_stream: UnixStream,
}

/// Commit fresh intent, authenticate the held journal stream, observe native Ready
/// and acknowledge that exact process. No detached acceptance task is created.
/// Executable approval and exclusion of replacement remain caller obligations.
/// Cancellation or any error retains unresolved ownership; it never grants retry.
/// # Errors
/// Refuses changed endpoint/launch, replay, missing Ready or failed durable ACK.
pub async fn prepare_native_broker(
    store: &Store,
    binding: &FileBrokerBinding,
    launch: &BrokerRunnerLaunch,
    wrapper: &Path,
    approved_sha256: &str,
    endpoint: BrokerEndpoint,
    now_ms: i64,
) -> Result<AcknowledgedBroker, BrokerError> {
    if endpoint
        .environment_value()
        .map_err(|_| BrokerError::Refused)?
        != launch.journal_handoff
    {
        return Err(BrokerError::Refused);
    }
    let prepare_id = Uuid::now_v7().to_string();
    let event = |id| EffectAudit {
        id,
        actor_id: "broker-launcher",
        correlation_id: &binding.invocation_id,
        now_ms,
    };
    record_broker_launch(
        store,
        binding,
        launch,
        wrapper,
        approved_sha256,
        event(&prepare_id),
    )
    .await?;
    approved_hash(wrapper, approved_sha256)?;
    let (runner, journal_stream) =
        tokio::try_join!(PreparedBrokerRunner::prepare(wrapper, launch), async {
            endpoint
                .accept(Duration::from_secs(10))
                .await
                .map_err(|_| BrokerError::Refused)
        })?;
    let observed = runner.identity();
    let execution = store
        .execution(&binding.execution_token)
        .await
        .map_err(|_| BrokerError::Admission)?
        .ok_or(BrokerError::Admission)?;
    let ack_id = Uuid::now_v7().to_string();
    store
        .update_execution(
            &binding.execution_token,
            execution.revision,
            &ack_id,
            &ExecutionUpdate::Acknowledge {
                wrapper_identity: binding.invocation_id.clone(),
                packet_sha256: execution.preparation.packet_sha256,
                process: Some(ExecutionProcess {
                    platform: observed.process.host.platform.clone(),
                    host_id: observed.process.host.host_id.clone(),
                    boot_id: observed.process.host.boot_id.clone(),
                    pid: observed.process.pid,
                    start_marker: observed.process.start_marker.clone(),
                }),
            },
            event(&ack_id),
        )
        .await
        .map_err(|_| BrokerError::Admission)?;
    Ok(AcknowledgedBroker {
        runner,
        journal_stream,
    })
}
