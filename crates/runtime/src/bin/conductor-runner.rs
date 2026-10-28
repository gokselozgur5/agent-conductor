//! Trusted wrapper: parse packet, acknowledge, wait for recorded release, supervise.
use conductor_runtime::{
    MAX_PACKET_BYTES, PacketAcknowledgement, ProcessError, ProcessSpec, RunnerControl, RunnerEvent,
    RunnerLaunch, continuous_millis, run_process,
};
use std::time::Duration;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    sync::watch,
};

#[tokio::main]
async fn main() {
    let status = if serve().await.is_err() {
        // No packet, environment, vendor output or credential material in errors.
        eprintln!("runner protocol or supervision failed");
        1
    } else {
        0
    };
    // Tokio stdin uses a blocking reader that cannot be cancelled. All owned
    // workers are settled before serve returns; do not wait for an idle parent
    // to close stdin merely to shut down that reader thread.
    std::process::exit(status);
}

async fn read_frame(reader: &mut (impl AsyncBufRead + Unpin), limit: usize) -> Result<Vec<u8>, ()> {
    let mut bytes = Vec::new();
    reader
        .take(u64::try_from(limit).map_err(|_| ())? + 1)
        .read_until(b'\n', &mut bytes)
        .await
        .map_err(|_| ())?;
    if bytes.is_empty() || bytes.len() > limit || bytes.last() != Some(&b'\n') {
        return Err(());
    }
    Ok(bytes)
}
async fn emit(event: &RunnerEvent) -> Result<(), ()> {
    let mut bytes = serde_json::to_vec(event).map_err(|_| ())?;
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut output = tokio::io::stdout();
        output.write_all(&bytes).await?;
        output.flush().await
    })
    .await
    .map_err(|_| ())?
    .map_err(|_| ())
}

#[allow(clippy::too_many_lines)]
async fn serve() -> Result<(), ()> {
    let mut input = BufReader::new(tokio::io::stdin());
    let bytes = tokio::time::timeout(
        Duration::from_secs(5),
        read_frame(&mut input, MAX_PACKET_BYTES),
    )
    .await
    .map_err(|_| ())??;
    let launch: RunnerLaunch = serde_json::from_slice(&bytes).map_err(|_| ())?;
    launch.packet.validate().map_err(|_| ())?;
    if launch.heartbeat_timeout_ms == 0
        || launch.heartbeat_timeout_ms > 30_000
        || launch.heartbeat_timeout_ms > launch.packet.contract.max_runtime_ms
        || launch.output_limit == 0
        || launch.output_limit > 1024 * 1024
        || !launch.executable.is_absolute()
    {
        return Err(());
    }
    let packet_hash = launch.packet.sha256().map_err(|_| ())?;
    let acknowledgement = PacketAcknowledgement {
        version: 1,
        attempt_id: launch.packet.contract.attempt_id.clone(),
        lease_epoch: launch.packet.contract.lease_epoch,
        packet_sha256: packet_hash.clone(),
        instruction_sha256: launch
            .packet
            .contract
            .instructions
            .iter()
            .map(|pin| pin.sha256.clone())
            .collect(),
    };
    emit(&RunnerEvent::Ready { acknowledgement }).await?;
    let release = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut input, 1024))
        .await
        .map_err(|_| ())??;
    let mut lease_deadline = match serde_json::from_slice::<RunnerControl>(&release) {
        Ok(RunnerControl::Release {
            packet_sha256,
            continuous_deadline_ms,
        }) if packet_sha256 == packet_hash => continuous_deadline_ms,
        _ => return Err(()),
    };
    let started = continuous_millis().map_err(|_| ())?;
    if started >= lease_deadline {
        return Err(());
    }
    let mut last_renewal = started;
    let max_runtime_ms = launch.packet.contract.max_runtime_ms;
    let specification = ProcessSpec {
        executable: launch.executable,
        arguments: launch.arguments,
        directory: launch.packet.contract.workspace,
        environment: launch.environment,
        timeout: Duration::from_millis(max_runtime_ms),
        output_limit: launch.output_limit,
    };
    let (cancel, receiver) = watch::channel(false);
    let mut process = tokio::spawn(async move { run_process(&specification, receiver).await });
    let mut timer = tokio::time::interval(Duration::from_millis(20));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Keep this future alive across timer ticks: cancelling a partial read would
    // lose frame bytes and could turn delayed input into a different message.
    let mut frame = Box::pin(read_frame(&mut input, 1024));
    let stopped = loop {
        tokio::select! {
            result = &mut process => {
                let result = result.map_err(|_| ())?;
                return emit(&match result {
                    Ok(output) => RunnerEvent::Finished { exit_code: output.exit_code, stdout: output.stdout, stderr: output.stderr },
                    Err(ProcessError::Cancelled | ProcessError::Timeout | ProcessError::OutputLimit) => RunnerEvent::Stopped { reason: "process_stopped".into() },
                    // Failed fencing or I/O is not evidence that the worker stopped.
                    Err(_) => return Err(()),
                }).await;
            },
            _ = timer.tick() => {
                match continuous_millis() {
                    Ok(now) if now >= last_renewal && now >= started
                        && now - last_renewal < launch.heartbeat_timeout_ms
                        && now - started < max_runtime_ms && now < lease_deadline => {},
                    _ => break "lease_or_runtime_expired",
                }
            },
            message = &mut frame => {
                match message.and_then(|bytes| serde_json::from_slice::<RunnerControl>(&bytes).map_err(|_| ())) {
                    Ok(RunnerControl::Renew { packet_sha256, continuous_deadline_ms }) if packet_sha256 == packet_hash => {
                        let Ok(now) = continuous_millis() else { break "clock_failed"; };
                        // A renewal received after expiry must not revive the worker.
                        if now < last_renewal || now - last_renewal >= launch.heartbeat_timeout_ms || now >= lease_deadline || now >= continuous_deadline_ms || now - started >= max_runtime_ms { break "late_renewal"; }
                        lease_deadline = continuous_deadline_ms;
                        last_renewal = now;
                    },
                    Ok(RunnerControl::Cancel) => break "cancelled",
                    _ => break "parent_lost_or_invalid_control",
                }
                drop(frame);
                frame = Box::pin(read_frame(&mut input, 1024));
            }
        }
    };
    let _ = cancel.send(true);
    match process.await.map_err(|_| ())? {
        Ok(_)
        | Err(ProcessError::Cancelled | ProcessError::Timeout | ProcessError::OutputLimit) => {}
        Err(_) => return Err(()),
    }
    emit(&RunnerEvent::Stopped {
        reason: stopped.into(),
    })
    .await
}
