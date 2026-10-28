//! Parent-controlled wrapper protocol. A subprocess cannot release itself.
use crate::{PacketAcknowledgement, ResumePacket};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerLaunch {
    pub packet: ResumePacket,
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub output_limit: usize,
    pub heartbeat_timeout_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunnerControl {
    Release { packet_sha256: String },
    Renew { packet_sha256: String },
    Cancel,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunnerEvent {
    Ready {
        acknowledgement: PacketAcknowledgement,
    },
    Finished {
        exit_code: Option<i32>,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
    Stopped {
        reason: String,
    },
}

/// Suspend-aware time for the wrapper watchdog. Clock failure must stop the worker.
/// # Errors
/// Rejects unsupported hosts, negative/overflow readings or OS failures.
pub fn continuous_millis() -> Result<u64, String> {
    use nix::time::{ClockId, clock_gettime};
    #[cfg(target_os = "linux")]
    let clock = ClockId::CLOCK_BOOTTIME;
    #[cfg(target_os = "macos")]
    let clock = ClockId::CLOCK_MONOTONIC;
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    return Err("unsupported continuous clock".into());
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let reading = clock_gettime(clock).map_err(|error| error.to_string())?;
        let seconds = u64::try_from(reading.tv_sec()).map_err(|_| "negative clock")?;
        let nanos = u64::try_from(reading.tv_nsec()).map_err(|_| "negative clock")?;
        seconds
            .checked_mul(1000)
            .and_then(|ms| ms.checked_add(nanos / 1_000_000))
            .ok_or_else(|| "continuous clock overflow".into())
    }
}
