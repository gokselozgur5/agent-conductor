//! Parent transport. Callers persist intent before spawn and identity before Release.
use crate::{AuxiliaryControl, AuxiliaryEvent, AuxiliaryLaunch, ChildIdentity};
use std::{io, path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::watch,
};

pub struct AuxiliarySession {
    child: Child,
    writer: ChildStdin,
    reader: BufReader<ChildStdout>,
    identity: ChildIdentity,
    hash: String,
    timeout_ms: u64,
    heartbeat_ms: u64,
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "auxiliary supervisor transport refused",
    )
}
async fn send(writer: &mut ChildStdin, value: &impl serde::Serialize) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(|_| invalid())?;
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(2), async {
        writer.write_all(&bytes).await?;
        writer.flush().await
    })
    .await
    .map_err(|_| invalid())?
}
async fn event(reader: &mut BufReader<ChildStdout>) -> io::Result<AuxiliaryEvent> {
    let mut bytes = Vec::new();
    reader
        .take(32 * 1024 * 1024 + 1)
        .read_until(b'\n', &mut bytes)
        .await?;
    if bytes.len() > 32 * 1024 * 1024 || bytes.last() != Some(&b'\n') {
        return Err(invalid());
    }
    serde_json::from_slice(&bytes).map_err(|_| invalid())
}
impl AuxiliarySession {
    /// Starts an approved pinned supervisor and observes Ready without Release.
    /// The caller must first commit this launch's digest and wrapper pin.
    /// # Errors
    /// Refuses mismatched launch/native identity or bounded protocol failures.
    pub async fn prepare(wrapper: &Path, launch: &AuxiliaryLaunch) -> io::Result<Self> {
        let hash = launch.sha256()?;
        let mut child = Command::new(wrapper)
            .args(["--auxiliary", &launch.invocation_id])
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(false)
            .spawn()?;
        let mut writer = child.stdin.take().ok_or_else(invalid)?;
        let mut reader = BufReader::new(child.stdout.take().ok_or_else(invalid)?);
        send(&mut writer, launch).await?;
        let ready = tokio::time::timeout(Duration::from_secs(12), event(&mut reader))
            .await
            .map_err(|_| invalid())??;
        let identity = ChildIdentity::observe(child.id().ok_or_else(invalid)?).await?;
        match ready {
            AuxiliaryEvent::Ready {
                launch_sha256,
                process,
            } if launch_sha256 == hash && process == identity => {}
            _ => return Err(invalid()),
        }
        identity.revalidate().await?;
        Ok(Self {
            child,
            writer,
            reader,
            identity,
            hash,
            timeout_ms: launch.timeout_ms,
            heartbeat_ms: launch.heartbeat_timeout_ms,
        })
    }
    #[must_use]
    pub fn identity(&self) -> &ChildIdentity {
        &self.identity
    }

    /// Sends Release only after the caller commits this exact observed identity.
    /// Dropping the session closes stdin; its independent wrapper owns cleanup.
    /// # Errors
    /// Transport/exit errors are not settlement proof. The caller must read the
    /// separately persisted receipt even when a terminal event is returned.
    pub async fn run(
        mut self,
        mut cancellation: watch::Receiver<bool>,
    ) -> io::Result<AuxiliaryEvent> {
        self.identity.revalidate().await?;
        if *cancellation.borrow() || cancellation.has_changed().is_err() {
            return Err(invalid());
        }
        send(
            &mut self.writer,
            &AuxiliaryControl::Release {
                launch_sha256: self.hash.clone(),
            },
        )
        .await?;
        let result = event(&mut self.reader);
        tokio::pin!(result);
        let mut renew =
            tokio::time::interval(Duration::from_millis((self.heartbeat_ms / 3).max(1)));
        renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let deadline = tokio::time::sleep(Duration::from_millis(self.timeout_ms + 10_000));
        tokio::pin!(deadline);
        let mut cancelled = false;
        let mut transport_failed = false;
        let drain_deadline = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(drain_deadline);
        let terminal = loop {
            tokio::select! {
                value=&mut result=>break value?,
                _=renew.tick(),if !cancelled && !transport_failed=>{
                    transport_failed = send(&mut self.writer,&AuxiliaryControl::Renew{launch_sha256:self.hash.clone()}).await.is_err();
                    if transport_failed {
                        drain_deadline.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(5));
                    }
                },
                _=cancellation.changed(),if !cancelled && !transport_failed=>{
                    cancelled=true;
                    transport_failed = send(&mut self.writer,&AuxiliaryControl::Cancel{launch_sha256:self.hash.clone()}).await.is_err();
                    if transport_failed {
                        drain_deadline.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(5));
                    }
                },
                ()=&mut drain_deadline,if transport_failed=>return Err(invalid()),
                ()=&mut deadline=>return Err(invalid()),
            }
        };
        if matches!(terminal, AuxiliaryEvent::Ready { .. }) {
            return Err(invalid());
        }
        let status = tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .map_err(|_| invalid())??;
        if !status.success() {
            return Err(invalid());
        }
        Ok(terminal)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    // A real pipe peer closes its control input after Release, keeps its output
    // open, and withholds the terminal frame until the parent opens the gate.
    async fn closed_control_peer(
        cancel: bool,
        suffix: &str,
        exit_code: &str,
    ) -> io::Result<AuxiliaryEvent> {
        let dir = tempfile::tempdir().unwrap();
        let script = r#"read -r release
read -r first_renewal
exec 0<&-
: > closed
while test ! -f gate; do sleep 0.01; done
if test "$1" = no-frame; then sleep 7; fi
printf '%s\n' "$1"
exit "$2"
"#;
        let mut child = Command::new("/bin/sh")
            .args(["-c", script, "peer", suffix, exit_code])
            .current_dir(dir.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let identity = ChildIdentity::observe(child.id().unwrap()).await.unwrap();
        let session = AuxiliarySession {
            writer: child.stdin.take().unwrap(),
            reader: BufReader::new(child.stdout.take().unwrap()),
            child,
            identity,
            hash: "a".repeat(64),
            timeout_ms: 1000,
            heartbeat_ms: if cancel { 60_000 } else { 15 },
        };
        let (sender, receiver) = watch::channel(false);
        let mut run = tokio::spawn(session.run(receiver));
        tokio::time::timeout(Duration::from_secs(3), async {
            while !dir.path().join("closed").exists() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        if cancel {
            sender.send(true).unwrap();
        }
        // No terminal frame exists yet. A control-write failure must leave the
        // existing bounded reader alive rather than discard its eventual result.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut run)
                .await
                .is_err(),
            "control failure discarded the pending terminal reader"
        );
        std::fs::write(dir.path().join("gate"), b"").unwrap();
        tokio::time::timeout(Duration::from_secs(8), run)
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn renewal_failure_drains_terminal_frame() {
        let result = closed_control_peer(
            false,
            r#"{"type":"finished","exit_code":0,"stdout":[],"stderr":[]}"#,
            "0",
        )
        .await
        .unwrap();
        assert!(matches!(
            result,
            AuxiliaryEvent::Finished {
                exit_code: Some(0),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn cancellation_failure_drains_terminal_frame() {
        let result = closed_control_peer(true, r#"{"type":"stopped","reason":"cancelled"}"#, "0")
            .await
            .unwrap();
        assert!(matches!(result, AuxiliaryEvent::Stopped { .. }));
    }
    #[tokio::test]
    async fn closed_control_does_not_accept_invalid_output_or_failed_exit() {
        assert!(closed_control_peer(false, "not-json", "0").await.is_err());
        assert!(
            closed_control_peer(
                false,
                r#"{"type":"finished","exit_code":0,"stdout":[],"stderr":[]}"#,
                "1"
            )
            .await
            .is_err()
        );
    }
    #[tokio::test]
    async fn broken_control_without_terminal_frame_has_bounded_drain() {
        let start = tokio::time::Instant::now();
        assert!(closed_control_peer(false, "no-frame", "0").await.is_err());
        assert!(start.elapsed() < Duration::from_secs(7));
    }
}
