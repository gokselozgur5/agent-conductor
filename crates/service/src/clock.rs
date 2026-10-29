use std::time::{SystemTime, UNIX_EPOCH};

use agent_conductor_domain::ClockReading;

use crate::Clock;

/// Host clock whose monotonic component continues across system suspend.
#[derive(Debug)]
pub struct SystemClock {
    origin_ms: u64,
}

impl SystemClock {
    /// Captures the operating system's suspend-aware monotonic origin.
    ///
    /// # Errors
    /// Fails closed when the supported host clock cannot be read.
    pub fn try_new() -> Result<Self, String> {
        continuous_now_ms().map(|origin_ms| Self { origin_ms })
    }
}

impl Clock for SystemClock {
    fn read(&self, boot_generation: u64) -> ClockReading {
        ClockReading {
            boot_generation,
            monotonic_ms: elapsed_or_expired(self.origin_ms, &continuous_now_ms()),
            wall_unix_ms: wall_ms(),
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn continuous_now_ms() -> Result<u64, String> {
    use nix::time::{ClockId, clock_gettime};

    // Linux clock_gettime(2): CLOCK_BOOTTIME includes suspended time.
    #[cfg(target_os = "linux")]
    let clock = ClockId::CLOCK_BOOTTIME;
    // Apple clock_gettime(3): CLOCK_MONOTONIC continues while the system sleeps;
    // CLOCK_UPTIME_RAW is the sleep-excluding clock on this platform.
    #[cfg(target_os = "macos")]
    let clock = ClockId::CLOCK_MONOTONIC;

    let reading = clock_gettime(clock).map_err(|error| error.to_string())?;
    let seconds = u64::try_from(reading.tv_sec()).map_err(|_| "negative clock".to_owned())?;
    let nanoseconds = u64::try_from(reading.tv_nsec()).map_err(|_| "negative clock".to_owned())?;
    seconds
        .checked_mul(1_000)
        .and_then(|millis| millis.checked_add(nanoseconds / 1_000_000))
        .ok_or_else(|| "continuous clock overflow".to_owned())
}

fn elapsed_or_expired(origin_ms: u64, reading: &Result<u64, String>) -> u64 {
    match reading {
        Ok(now_ms) if *now_ms >= origin_ms => *now_ms - origin_ms,
        Ok(_) | Err(_) => u64::MAX,
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn continuous_now_ms() -> Result<u64, String> {
    Err("suspend-aware monotonic clock is unsupported on this host".to_owned())
}

pub(super) fn wall_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_host_clock_is_monotonic() {
        let clock = SystemClock::try_new().expect("supported host clock");
        let first = clock.read(7);
        let second = clock.read(7);
        assert_eq!(first.boot_generation, 7);
        assert!(second.monotonic_ms >= first.monotonic_ms);
        assert!(second.wall_unix_ms > 0);
    }

    #[test]
    fn clock_failure_or_regression_expires_leases_fail_closed() {
        assert_eq!(elapsed_or_expired(100, &Ok(150)), 50);
        assert_eq!(elapsed_or_expired(100, &Ok(99)), u64::MAX);
        assert_eq!(
            elapsed_or_expired(100, &Err("unavailable".into())),
            u64::MAX
        );
    }
}
