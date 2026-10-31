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
const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_LAST: Duration = Duration::from_secs(30);

pub async fn serve(service: PersistentControlService, stop: tokio::sync::watch::Receiver<bool>) {
    supervise(
        |stop| service.dispatch_next_conversation(stop),
        stop,
        IDLE,
        (RETRY_FIRST, RETRY_LAST),
    )
    .await;
}

/// Doubling delay between failed passes, bounded by a ceiling.
struct Backoff {
    first: Duration,
    last: Duration,
    current: Duration,
}

impl Backoff {
    fn new(first: Duration, last: Duration) -> Self {
        Self {
            first,
            last,
            current: first,
        }
    }

    fn next(&mut self) -> Duration {
        let wait = self.current;
        self.current = (wait * 2).min(self.last);
        wait
    }

    fn reset(&mut self) {
        self.current = self.first;
    }
}

/// Runs passes until shutdown. After an error the loop waits `retry.0`, doubling
/// up to `retry.1` while errors continue, and starts over after a success.
async fn supervise<Step, Pass, Failure>(
    mut attempt: Step,
    mut stop: tokio::sync::watch::Receiver<bool>,
    idle: Duration,
    retry: (Duration, Duration),
) where
    Step: FnMut(tokio::sync::watch::Receiver<bool>) -> Pass,
    Pass: std::future::Future<Output = Result<bool, Failure>>,
    Failure: std::fmt::Display,
{
    let mut backoff = Backoff::new(retry.0, retry.1);
    while !*stop.borrow() {
        let wait = match attempt(stop.clone()).await {
            Ok(true) => {
                backoff.reset();
                continue;
            }
            Ok(false) => {
                backoff.reset();
                idle
            }
            Err(error) => {
                let wait = backoff.next();
                eprintln!("conversation dispatcher pass failed: {error}; retrying in {wait:?}");
                wait
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
            (FAST, FAST),
        )
        .await;
        assert_eq!(passes.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn backoff_doubles_to_its_ceiling_and_resets() {
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(30));
        let waits: Vec<u64> = (0..7).map(|_| backoff.next().as_secs()).collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 30, 30]);
        backoff.reset();
        assert_eq!(backoff.next(), Duration::from_secs(1));
    }
}
