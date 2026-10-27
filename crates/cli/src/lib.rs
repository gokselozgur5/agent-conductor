use std::io;
use std::path::Path;

use conductor_protocol::{RequestEnvelope, ResponseEnvelope};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

const MAX_RESPONSE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("daemon I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("request serialization failed: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("response decoding failed: {0}")]
    Decode(#[source] serde_json::Error),
    #[error("daemon response exceeds frame limit")]
    FrameTooLarge,
    #[error("daemon response does not match request")]
    MismatchedResponse,
}

/// Sends one request to the local daemon and validates response correlation.
///
/// # Errors
///
/// Returns a structured client error for transport, framing, JSON, or correlation
/// failures.
pub async fn send(
    socket: impl AsRef<Path>,
    request: &RequestEnvelope,
) -> Result<ResponseEnvelope, ClientError> {
    let mut stream = UnixStream::connect(socket).await?;
    let body = serde_json::to_vec(request).map_err(ClientError::Encode)?;
    stream.write_all(&body).await?;
    stream.shutdown().await?;

    let mut response = Vec::new();
    (&mut stream)
        .take(MAX_RESPONSE_BYTES + 1)
        .read_to_end(&mut response)
        .await?;
    if response.len() as u64 > MAX_RESPONSE_BYTES {
        return Err(ClientError::FrameTooLarge);
    }

    let response: ResponseEnvelope =
        serde_json::from_slice(&response).map_err(ClientError::Decode)?;
    if response.request_message_id != Some(request.message_id)
        || response.correlation_id != request.correlation_id
    {
        return Err(ClientError::MismatchedResponse);
    }
    Ok(response)
}
