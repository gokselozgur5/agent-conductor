//! Durable conversation queue supervision. Never drop a running native future
//! on shutdown: signal cancellation and await its persisted terminal observation.
use conductor_service::{ConversationAgent, PersistentControlService};
use std::{io::Read, path::Path, time::Duration};

pub fn configure(
    service: &PersistentControlService,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(262_145)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 262_144 {
        return Err("conversation configuration too large".into());
    }
    let agents: Vec<ConversationAgent> = serde_json::from_slice(&bytes)?;
    service.configure_conversation_agents(agents)?;
    Ok(())
}

/// A failed pass never ends supervision: queued chat for every conversation
/// depends on this loop, and a transient store error must not outlive its cause.
const IDLE: Duration = Duration::from_millis(250);
const RETRY: Duration = Duration::from_secs(1);

pub async fn serve(service: PersistentControlService, stop: tokio::sync::watch::Receiver<bool>) {
    supervise(
        |stop| service.dispatch_next_conversation(stop),
        stop,
        IDLE,
        RETRY,
    )
    .await;
}

/// Runs passes until shutdown. After an error the loop waits `retry` and goes on.
async fn supervise<Step, Pass, Failure>(
    mut attempt: Step,
    mut stop: tokio::sync::watch::Receiver<bool>,
    idle: Duration,
    retry: Duration,
) where
    Step: FnMut(tokio::sync::watch::Receiver<bool>) -> Pass,
    Pass: std::future::Future<Output = Result<bool, Failure>>,
    Failure: std::fmt::Display,
{
    while !*stop.borrow() {
        let wait = match attempt(stop.clone()).await {
            Ok(true) => continue,
            Ok(false) => idle,
            Err(error) => {
                eprintln!("conversation dispatcher pass failed: {error}; retrying in {retry:?}");
                retry
            }
        };
        tokio::select! {
            _ = stop.changed() => return,
            () = tokio::time::sleep(wait) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const FAST: Duration = Duration::from_millis(1);

    #[tokio::test]
    async fn an_error_in_one_pass_does_not_stop_later_passes() {
        let (stop_sender, stop) = tokio::sync::watch::channel(false);
        let passes = AtomicUsize::new(0);
        supervise(
            |_| async {
                match passes.fetch_add(1, Ordering::SeqCst) {
                    0 | 1 => Err("database is busy"),
                    4 => {
                        stop_sender.send(true).unwrap();
                        Ok(false)
                    }
                    _ => Ok(true),
                }
            },
            stop,
            FAST,
            FAST,
        )
        .await;
        assert_eq!(passes.load(Ordering::SeqCst), 5);
    }
}
