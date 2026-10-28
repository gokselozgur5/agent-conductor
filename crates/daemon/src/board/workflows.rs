use super::{BoardState, query};
use axum::{
    body::to_bytes,
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use conductor_protocol::{ErrorCode, Request as ControlRequest};
use conductor_service::ControlService;
use std::time::Duration;
pub(super) async fn list<S: ControlService>(State(state): State<BoardState<S>>) -> Response {
    query(&state, ControlRequest::ListWorkflows).await
}
pub(super) async fn submit<S: ControlService>(
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
        to_bytes(request.into_body(), 131_072),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
    };
    let Ok(plan_json) = String::from_utf8(bytes.to_vec()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    match tokio::time::timeout(
        Duration::from_secs(3),
        state
            .service
            .handle(ControlRequest::SubmitWorkflow { plan_json }),
    )
    .await
    {
        Ok(Ok(_)) => StatusCode::ACCEPTED.into_response(),
        Ok(Err(error)) => match error.code {
            ErrorCode::Conflict => StatusCode::CONFLICT,
            ErrorCode::Validation
            | ErrorCode::InvariantViolation
            | ErrorCode::UnsupportedProtocol => StatusCode::BAD_REQUEST,
            ErrorCode::Internal | ErrorCode::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
        .into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
