//! Native boot identity, independent of wall clock and daemon lifetime.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostEpoch {
    pub platform: String,
    pub host_id: String,
    pub boot_id: String,
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "native host/boot identity unavailable",
    )
}

fn normalized_id(value: &str) -> io::Result<String> {
    let value = value.trim();
    if value.len() != 32 && value.len() != 36 {
        return Err(invalid());
    }
    let parsed = uuid::Uuid::parse_str(value).map_err(|_| invalid())?;
    if parsed.is_nil() {
        return Err(invalid());
    }
    Ok(parsed.simple().to_string())
}

impl HostEpoch {
    fn from_native(platform: &str, machine: &str, boot: &str) -> io::Result<Self> {
        let machine = normalized_id(machine)?;
        let boot = normalized_id(boot)?;
        Ok(Self {
            platform: platform.into(),
            host_id: format!(
                "{:x}",
                Sha256::digest(format!("conductor-host-v1\0{platform}\0{machine}"))
            ),
            boot_id: format!(
                "{:x}",
                Sha256::digest(format!("conductor-boot-v1\0{platform}\0{boot}"))
            ),
        })
    }

    /// Checks the normalized platform and digest shapes.
    /// # Errors
    /// Returns an error for malformed identity fields.
    pub fn validate(&self) -> io::Result<()> {
        if !matches!(self.platform.as_str(), "linux" | "macos")
            || [&self.host_id, &self.boot_id]
                .iter()
                .any(|value| value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(invalid());
        }
        Ok(())
    }

    /// Compares the host identity independently of its current boot.
    #[must_use]
    pub fn same_host(&self, other: &Self) -> bool {
        self.platform == other.platform && self.host_id == other.host_id
    }

    #[cfg(target_os = "linux")]
    /// Reads native host and boot identities with bounded input.
    /// # Errors
    /// Returns an error when the platform cannot establish its native identity.
    pub async fn current() -> io::Result<Self> {
        use tokio::io::AsyncReadExt;
        async fn bounded(path: &str) -> io::Result<String> {
            let mut text = String::new();
            tokio::fs::File::open(path)
                .await?
                .take(65)
                .read_to_string(&mut text)
                .await?;
            if text.len() > 64 {
                return Err(invalid());
            }
            Ok(text)
        }
        Self::from_native(
            "linux",
            &bounded("/etc/machine-id").await?,
            &bounded("/proc/sys/kernel/random/boot_id").await?,
        )
    }

    #[cfg(target_os = "macos")]
    /// Reads native host and boot identities with bounded input.
    /// # Errors
    /// Returns an error when the platform cannot establish its native identity.
    pub async fn current() -> io::Result<Self> {
        let registry =
            native_command("/usr/sbin/ioreg", &["-rd1", "-c", "IOPlatformExpertDevice"]).await?;
        let machines: Vec<_> = registry
            .lines()
            .filter_map(|line| {
                let (key, value) = line.trim().split_once(" = ")?;
                (key == "\"IOPlatformUUID\"").then_some(value.trim_matches('"'))
            })
            .collect();
        if machines.len() != 1 {
            return Err(invalid());
        }
        let boot = native_command("/usr/sbin/sysctl", &["-n", "kern.bootsessionuuid"]).await?;
        Self::from_native("macos", machines[0], &boot)
    }
}

#[cfg(target_os = "macos")]
async fn native_command(program: &str, args: &[&str]) -> io::Result<String> {
    use tokio::io::AsyncReadExt;
    let mut child = tokio::process::Command::new(program)
        .env_clear()
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        child
            .stdout
            .take()
            .ok_or_else(invalid)?
            .take(65537)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() > 65536 {
            return Err(invalid());
        }
        if !child.wait().await?.success() {
            return Err(invalid());
        }
        String::from_utf8(bytes).map_err(|_| invalid())
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "native identity command timed out"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn native_epoch_is_valid_and_stable_within_this_boot() {
        let first = HostEpoch::current().await.unwrap();
        first.validate().unwrap();
        assert_eq!(first, HostEpoch::current().await.unwrap());
    }

    #[test]
    fn identity_normalizes_case_but_keeps_machine_and_boot_separate() {
        let a = "12345678-1234-1234-1234-123456789abc";
        let b = "22345678-1234-1234-1234-123456789abc";
        let first = HostEpoch::from_native("macos", a, a).unwrap();
        assert_eq!(
            first,
            HostEpoch::from_native("macos", &a.to_uppercase(), a).unwrap()
        );
        let reboot = HostEpoch::from_native("macos", a, b).unwrap();
        assert!(first.same_host(&reboot));
        assert_ne!(first.boot_id, reboot.boot_id);
        assert!(!first.same_host(&HostEpoch::from_native("macos", b, a).unwrap()));
        for bad in [
            "",
            "00000000000000000000000000000000",
            "invalid",
            "12345678-1234-1234-1234-123456789abc trailing",
        ] {
            assert!(HostEpoch::from_native("linux", bad, a).is_err());
        }
    }
}
