//! Trusted wrapper: parse packet, acknowledge, wait for recorded release, supervise.
use conductor_runtime::{
    MAX_PACKET_BYTES, PacketAcknowledgement, ProcessError, ProcessSpec, RunnerControl, RunnerEvent,
    RunnerLaunch, continuous_millis, run_process_observed,
};
use std::time::Duration;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    sync::watch,
};

#[tokio::main]
async fn main() {
    let arguments: Vec<String> = std::env::args().collect();
    let result = if arguments.get(1).is_some_and(|v| v == "--file-broker") {
        if arguments.len() == 2 {
            conductor_runtime::serve_broker_runner()
                .await
                .map_err(|_| ())
        } else {
            Err(())
        }
    } else if arguments.get(1).is_some_and(|v| v == "--auxiliary") {
        if matches!(arguments.len(), 3 | 4) {
            conductor_runtime::serve_auxiliary_tagged(
                &arguments[2],
                arguments.get(3).map(String::as_str),
            )
            .await
            .map_err(|_| ())
        } else {
            Err(())
        }
    } else if arguments.get(1).is_some_and(|v| v == "--account-listen") {
        serve_account_channel(&arguments).await
    } else if arguments.get(1).is_some_and(|v| v == "--account-native") {
        serve(true).await
    } else {
        serve(false).await
    };
    let status = if result.is_err() {
        // No packet, environment, vendor output or credential material in errors.
        eprintln!("runner protocol or supervision failed");
        1
    } else {
        0
    };
    // Tokio stdin uses a blocking reader that cannot be cancelled. Handlers
    // finish cleanup attempts before returning; errors can leave uncertainty,
    // so wrapper exit alone is never settlement evidence. Do not wait for an
    // idle parent to close stdin merely to shut down that reader thread.
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
type Output = std::sync::Arc<tokio::sync::Mutex<Box<dyn tokio::io::AsyncWrite + Unpin + Send>>>;

async fn emit(output: &Output, event: &RunnerEvent) -> Result<(), ()> {
    let mut bytes = serde_json::to_vec(event).map_err(|_| ())?;
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut output = output.lock().await;
        output.write_all(&bytes).await?;
        output.flush().await
    })
    .await
    .map_err(|_| ())?
    .map_err(|_| ())
}

async fn emit_terminal(
    output: &Output,
    packet: &conductor_runtime::ResumePacket,
    event: &RunnerEvent,
) -> Result<(), ()> {
    conductor_runtime::write_terminal_receipt(packet, event).map_err(|_| ())?;
    emit(output, event).await
}

async fn serve(account_native: bool) -> Result<(), ()> {
    let identity = conductor_runtime::RunnerProcessIdentity::from_arguments(
        std::env::args_os().skip(if account_native { 2 } else { 1 }),
    )
    .map_err(|_| ())?;
    serve_io(
        account_native,
        false,
        identity,
        BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
    )
    .await
}

async fn serve_account_channel(arguments: &[String]) -> Result<(), ()> {
    if arguments.len() != 5 {
        return Err(());
    }
    let coordinator_uid = arguments[3].parse::<u32>().map_err(|_| ())?;
    let identity =
        conductor_runtime::RunnerProcessIdentity::from_arguments(std::env::args_os().skip(4))
            .map_err(|_| ())?
            .ok_or(())?;
    let endpoint = conductor_runtime::AccountChannelListener::bind(
        std::path::Path::new(&arguments[2]),
        coordinator_uid,
        identity.packet_sha256.clone(),
    )
    .map_err(|_| ())?;
    let stream = endpoint
        .accept(Duration::from_secs(30))
        .await
        .map_err(|_| ())?;
    let (input, output) = stream.into_split();
    serve_io(true, true, Some(identity), BufReader::new(input), output).await
}

#[allow(clippy::too_many_lines)]
async fn serve_io(
    account_native: bool,
    account_channel: bool,
    identity: Option<conductor_runtime::RunnerProcessIdentity>,
    mut input: impl AsyncBufRead + Unpin,
    writer: impl tokio::io::AsyncWrite + Unpin + Send + 'static,
) -> Result<(), ()> {
    let output: Output = std::sync::Arc::new(tokio::sync::Mutex::new(Box::new(writer)));
    let bytes = tokio::time::timeout(
        Duration::from_secs(5),
        read_frame(&mut input, MAX_PACKET_BYTES),
    )
    .await
    .map_err(|_| ())??;
    let launch: RunnerLaunch = if account_channel {
        let request: conductor_runtime::AccountRunnerRequest =
            serde_json::from_slice(&bytes).map_err(|_| ())?;
        if identity.as_ref() != Some(&request.identity) {
            return Err(());
        }
        request.launch.prepare().map_err(|_| ())?
    } else if account_native {
        serde_json::from_slice::<conductor_runtime::AccountNativeLaunch>(&bytes)
            .map_err(|_| ())?
            .prepare()
            .map_err(|_| ())?
    } else {
        serde_json::from_slice(&bytes).map_err(|_| ())?
    };
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
    conductor_runtime::read_pinned_instructions(&launch.packet).map_err(|_| ())?;
    let packet_hash = launch.packet.sha256().map_err(|_| ())?;
    if identity
        .as_ref()
        .is_some_and(|identity| identity.packet_sha256 != packet_hash)
    {
        return Err(());
    }
    let receipt_packet = launch.packet.clone();
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
    let Ok((started, mut lease_deadline)) =
        prepare_release(&output, &launch, &mut input, acknowledgement, &packet_hash).await
    else {
        // This return is before any worker task/spawn. Publication failure
        // stays unknown; publication success must never reach the spawn path.
        if let Some(identity) = identity.as_ref() {
            conductor_runtime::write_unstarted_receipt(&receipt_packet, identity)
                .map_err(|_| ())?;
        }
        return Err(());
    };
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
    // Progress output has its own bounded queue. A slow parent cannot stall the
    // watchdog/control loop, and only this task writes progress frames.
    let (progress, mut progress_events) = tokio::sync::mpsc::channel(8);
    let checkpoint_output = output.clone();
    let progress_output = tokio::spawn(async move {
        while let Some(claims) = progress_events.recv().await {
            emit(&checkpoint_output, &RunnerEvent::Checkpoint { claims }).await?;
        }
        Ok::<_, ()>(())
    });
    let mut parser = conductor_runtime::CheckpointStream::new(receipt_packet.clone());
    let mut process = tokio::spawn(async move {
        let mut captured = Vec::new();
        let result = run_process_observed(&specification, receiver, &mut captured, &mut |bytes| {
            if let Some(claims) = parser.observe(bytes) {
                let _ = progress.try_send(claims);
            }
        })
        .await;
        (result, captured)
    });
    let mut timer = tokio::time::interval(Duration::from_millis(20));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Keep this future alive across timer ticks: cancelling a partial read would
    // lose frame bytes and could turn delayed input into a different message.
    let mut frame = Box::pin(read_frame(&mut input, 1024));
    let stopped = loop {
        tokio::select! {
            result = &mut process => {
                let (result, captured) = result.map_err(|_| ())?;
                progress_output.await.map_err(|_| ())??;
                return emit_terminal(&output,&receipt_packet,&match result {
                    Ok(output) => RunnerEvent::Finished { exit_code: output.exit_code, stdout: output.stdout, stderr: output.stderr },
                    Err(ProcessError::Cancelled | ProcessError::Timeout | ProcessError::OutputLimit) => RunnerEvent::Stopped { reason: "process_stopped".into(), stdout: captured },
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
                        if conductor_runtime::read_pinned_instructions(&receipt_packet).is_err() { break "instruction_changed_or_unavailable"; }
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
    let (result, captured) = process.await.map_err(|_| ())?;
    progress_output.await.map_err(|_| ())??;
    match result {
        // Cancellation can race a normal exit. Preserve the weaker observation:
        // normal exit did not fence descendants and must never become Stopped.
        Ok(process_output) => {
            return emit_terminal(
                &output,
                &receipt_packet,
                &RunnerEvent::Finished {
                    exit_code: process_output.exit_code,
                    stdout: process_output.stdout,
                    stderr: process_output.stderr,
                },
            )
            .await;
        }
        Err(ProcessError::Cancelled | ProcessError::Timeout | ProcessError::OutputLimit) => {}
        Err(_) => return Err(()),
    }
    emit_terminal(
        &output,
        &receipt_packet,
        &RunnerEvent::Stopped {
            reason: stopped.into(),
            stdout: captured,
        },
    )
    .await
}

async fn prepare_release(
    output: &Output,
    launch: &RunnerLaunch,
    input: &mut (impl AsyncBufRead + Unpin),
    acknowledgement: PacketAcknowledgement,
    packet_hash: &str,
) -> Result<(u64, u64), ()> {
    emit(output, &RunnerEvent::Ready { acknowledgement }).await?;
    let release = tokio::time::timeout(Duration::from_secs(5), read_frame(input, 1024))
        .await
        .map_err(|_| ())??;
    let lease_deadline = match serde_json::from_slice::<RunnerControl>(&release) {
        Ok(RunnerControl::Release {
            packet_sha256,
            continuous_deadline_ms,
        }) if packet_sha256 == packet_hash => continuous_deadline_ms,
        _ => return Err(()),
    };
    conductor_runtime::read_pinned_instructions(&launch.packet).map_err(|_| ())?;
    let started = continuous_millis().map_err(|_| ())?;
    if started >= lease_deadline {
        return Err(());
    }
    Ok((started, lease_deadline))
}
