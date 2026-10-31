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

    #[tokio::test(start_paused = true)]
    async fn failed_passes_wait_one_two_then_up_to_thirty_seconds_and_success_resets() {
        let (stop_sender, stop) = tokio::sync::watch::channel(false);
        let start = tokio::time::Instant::now();
        let calls = std::sync::Mutex::new(Vec::new());
        // Seven failures, a busy success, a failure after it, then shutdown.
        supervise(
            |_| async {
                let mut calls = calls.lock().unwrap();
                calls.push(start.elapsed().as_secs());
                match calls.len() {
                    1..=7 | 9 => Err("database is busy"),
                    8 => Ok(true),
                    _ => {
                        stop_sender.send(true).unwrap();
                        Ok(false)
                    }
                }
            },
            stop,
            Duration::from_millis(250),
            (RETRY_FIRST, RETRY_LAST),
        )
        .await;
        // Attempt times: each gap is the wait that followed the earlier failure.
        assert_eq!(
            calls.into_inner().unwrap(),
            [0, 1, 3, 7, 15, 31, 61, 91, 91, 92]
        );
    }
}
