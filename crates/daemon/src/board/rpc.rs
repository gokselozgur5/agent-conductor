//! The existing control protocol over authenticated loopback HTTP. Possession of
//! the operator credential grants the same authority as the local control socket.
use super::BoardState;
use axum::{
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use conductor_protocol::{ErrorBody, ErrorCode, RequestEnvelope, ResponseEnvelope};
use conductor_service::ControlService;
use std::time::Duration;

pub(super) async fn apply<S: ControlService>(
    State(state): State<BoardState<S>>,
    request: Request,
) -> Response {
    if request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        != Some("application/json")
    {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    }
    let bytes = match tokio::time::timeout(
        Duration::from_secs(3),
        to_bytes(request.into_body(), 1024 * 1024),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
    };
    let Ok(request) = serde_json::from_slice::<RequestEnvelope>(&bytes) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let response = match request.validate() {
        Err(error) => ResponseEnvelope::failure(Some(&request), error.into()),
        Ok(()) => match tokio::time::timeout(
            Duration::from_secs(3),
            state.service.handle(request.payload.clone()),
        )
        .await
        {
            Ok(Ok(payload)) => ResponseEnvelope::success(&request, payload),
            Ok(Err(error)) => ResponseEnvelope::failure(Some(&request), error),
            Err(_) => ResponseEnvelope::failure(
                Some(&request),
                ErrorBody::new(
                    ErrorCode::Unavailable,
                    "request timed out; reconcile before retrying",
                ),
            ),
        },
    };
    match crate::response_encoding::encode(response, 1024 * 1024).await {
        Ok(bytes) => (
            [(header::CONTENT_TYPE, "application/json")],
            Body::from(bytes),
        )
            .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
