//! Parent-controlled wrapper protocol. A subprocess cannot release itself.
use crate::{PacketAcknowledgement, ResumePacket};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerLaunch {
    pub packet: ResumePacket,
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub output_limit: usize,
    pub heartbeat_timeout_ms: u64,
}

impl std::fmt::Debug for RunnerLaunch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunnerLaunch")
            .field("executable", &self.executable)
            .field("argument_count", &self.arguments.len())
            .field(
                "environment_keys",
                &self.environment.keys().collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunnerControl {
    Release {
        packet_sha256: String,
        continuous_deadline_ms: u64,
    },
    Renew {
        packet_sha256: String,
        continuous_deadline_ms: u64,
    },
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
