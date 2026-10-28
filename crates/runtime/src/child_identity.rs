//! Parent-observed kernel metadata. A persisted marker is not a signalable handle.
use crate::HostEpoch;
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildIdentity {
    pub host: HostEpoch,
    pub pid: u32,
    pub start_marker: String,
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "child process identity unavailable",
    )
}

impl ChildIdentity {
    /// Samples native metadata; the caller must separately retain its unreaped child.
    /// # Errors
    /// Refuses missing, malformed or unsupported native observations.
    pub async fn observe(pid: u32) -> io::Result<Self> {
        if pid == 0 || pid > i32::MAX.unsigned_abs() {
            return Err(invalid());
        }
        let host = HostEpoch::current().await?;
        let start_marker = start_marker(pid).await?;
        Ok(Self {
            host,
            pid,
            start_marker,
        })
    }

    /// Rechecks the kernel marker while the parent still owns its child.
    /// # Errors
    /// Refuses changed or unavailable observations. Equality never permits signaling.
    pub async fn revalidate(&self) -> io::Result<()> {
        if start_marker(self.pid).await? != self.start_marker {
            return Err(invalid());
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
async fn start_marker(pid: u32) -> io::Result<String> {
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    tokio::fs::File::open(format!("/proc/{pid}/stat"))
        .await?
        .take(65537)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 65536 {
        return Err(invalid());
    }
    linux_marker(&bytes, pid)
}

#[cfg(any(target_os = "linux", test))]
fn linux_marker(bytes: &[u8], pid: u32) -> io::Result<String> {
    let text = std::str::from_utf8(bytes).map_err(|_| invalid())?;
    let (prefix, suffix) = text.rsplit_once(") ").ok_or_else(invalid)?;
    if !prefix.starts_with(&format!("{pid} (")) {
        return Err(invalid());
    }
    let raw = suffix
        .split_ascii_whitespace()
        .nth(19)
        .ok_or_else(invalid)?;
    if raw.len() > 20 || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    let ticks = raw.parse::<u64>().map_err(|_| invalid())?;
    Ok(format!("linux-start-ticks:{ticks}"))
}

#[cfg(target_os = "macos")]
async fn start_marker(pid: u32) -> io::Result<String> {
    let pid = i32::try_from(pid).map_err(|_| invalid())?;
    let info = tokio::task::spawn_blocking(move || {
        libproc::proc_pid::pidinfo::<libproc::bsd_info::BSDInfo>(pid, 0)
    })
    .await
    .map_err(|_| invalid())?
    .map_err(|_| invalid())?;
    if info.pbi_pid != u32::try_from(pid).map_err(|_| invalid())?
        || info.pbi_start_tvsec == 0
        || info.pbi_start_tvusec >= 1_000_000
    {
        return Err(invalid());
    }
    Ok(format!(
        "macos-start-us:{}:{}",
        info.pbi_start_tvsec, info.pbi_start_tvusec
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_comm_parentheses_do_not_shift_start_field() {
        let tail = format!("S {} 12345", ["0"; 18].join(" "));
        let raw = format!("42 (a ) strange) {tail}");
        assert_eq!(
            linux_marker(raw.as_bytes(), 42).unwrap(),
            "linux-start-ticks:12345"
        );
        assert!(linux_marker(raw.as_bytes(), 43).is_err());
        for bad in ["", "42 (x) S", "42 (x) S -1"] {
            assert!(linux_marker(bad.as_bytes(), 42).is_err());
        }
    }

    #[tokio::test]
    async fn owned_native_child_retains_identity_until_reaped() {
        let mut child = tokio::process::Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();
        let identity = ChildIdentity::observe(pid).await.unwrap();
        identity.revalidate().await.unwrap();
        assert!(child.try_wait().unwrap().is_none());
        drop(child.stdin.take());
        assert!(child.wait().await.unwrap().success());
        assert!(identity.revalidate().await.is_err());
    }
}
