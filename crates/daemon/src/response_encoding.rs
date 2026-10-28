//! Bounded CPU work, separate from the socket's write deadline.
use conductor_protocol::{ErrorBody, ErrorCode, ResponseEnvelope, ResponseResult};
use std::{io, sync::Arc, sync::LazyLock};
use tokio::sync::Semaphore;

// Process-wide, including overlapping server lifetimes. Cancellation must not
// release this capacity while a non-abortable blocking closure is still alive.
static ENCODERS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(4)));

pub(super) async fn encode(response: ResponseEnvelope, limit: usize) -> io::Result<Vec<u8>> {
    bounded(Arc::clone(&ENCODERS), move || encode_frame(response, limit)).await
}

async fn bounded<F>(permits: Arc<Semaphore>, work: F) -> io::Result<Vec<u8>>
where
    F: FnOnce() -> io::Result<Vec<u8>> + Send + 'static,
{
    let permit = permits.acquire_owned().await.map_err(io::Error::other)?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(io::Error::other)?
}

fn encode_frame(mut response: ResponseEnvelope, limit: usize) -> io::Result<Vec<u8>> {
    let mut output = serde_json::to_vec(&response).map_err(io::Error::other)?;
    if output.len().saturating_add(1) > limit {
        response.result = ResponseResult::Error(ErrorBody::new(
            ErrorCode::Unavailable,
            "response exceeds frame limit",
        ));
        output = serde_json::to_vec(&response).map_err(io::Error::other)?;
    }
    if output.len().saturating_add(1) > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "response frame limit cannot contain an error envelope",
        ));
    }
    output.push(b'\n');
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn cancellation_retains_capacity_until_blocking_work_exits() {
        let permits = Arc::new(Semaphore::new(1));
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let first = tokio::spawn(bounded(Arc::clone(&permits), move || {
            started.send(()).unwrap();
            blocked.recv_timeout(Duration::from_secs(2)).unwrap();
            Ok(vec![1])
        }));
        ready.await.unwrap();
        // This current-thread runtime must progress while encoding is blocked.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!first.is_finished());
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(permits.available_permits(), 0);
        let (entered, mut next) = tokio::sync::oneshot::channel();
        let second = tokio::spawn(bounded(Arc::clone(&permits), move || {
            entered.send(()).unwrap();
            Ok(vec![2])
        }));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut next)
                .await
                .is_err()
        );
        second.abort();
        assert!(second.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        let third = bounded(Arc::clone(&permits), || Ok(vec![3]));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), third)
                .await
                .unwrap()
                .unwrap(),
            vec![3]
        );
        assert!(next.await.is_err(), "cancelled queued work must never run");
        assert_eq!(permits.available_permits(), 1);
    }
}
