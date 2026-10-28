//! Narrow operator command admission; lifecycle rules remain in the domain.
use super::BoardState;
use agent_conductor_domain::{
    Actor, ActorId, ActorRole, Command, CommandKind, CorrelationId, IdempotencyKey,
};
use axum::{
    body::to_bytes,
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use conductor_protocol::{ErrorCode, Request as ControlRequest};
use conductor_service::ControlService;
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    task_id: String,
    expected_version: u64,
    idempotency_key: String,
    action: Action,
    decision_id: Option<String>,
    question: Option<String>,
    rationale: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Ready,
    Pause,
    Resume,
    Cancel,
    OpenDecision,
    ResolveDecision,
}

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
        to_bytes(request.into_body(), 16_384),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
    };
    let Ok(input) = serde_json::from_slice::<Input>(&bytes) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if input.task_id.trim().is_empty()
        || input.task_id.len() > 256
        || input.idempotency_key.is_empty()
        || input.idempotency_key.len() > 128
        || !input
            .idempotency_key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let has_decision_fields =
        input.decision_id.is_some() || input.question.is_some() || input.rationale.is_some();
    if has_decision_fields
        && !matches!(input.action, Action::OpenDecision | Action::ResolveDecision)
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let kind = match input.action {
        Action::Ready => CommandKind::MarkReady,
        Action::Pause => CommandKind::Pause,
        Action::Resume => CommandKind::Resume,
        Action::Cancel => CommandKind::Cancel,
        Action::OpenDecision => {
            let (Some(id), Some(question), None) =
                (input.decision_id, input.question, input.rationale)
            else {
                return StatusCode::BAD_REQUEST.into_response();
            };
            CommandKind::OpenDecision { id, question }
        }
        Action::ResolveDecision => {
            let (Some(id), None, Some(rationale)) =
                (input.decision_id, input.question, input.rationale)
            else {
                return StatusCode::BAD_REQUEST.into_response();
            };
            CommandKind::ResolveDecision { id, rationale }
        }
    };
    let request = ControlRequest::ApplyCommand {
        task_id: input.task_id,
        command: Command {
            actor: Actor {
                id: ActorId("local-board-operator".into()),
                role: ActorRole::Operator,
            },
            expected_version: input.expected_version,
            correlation_id: CorrelationId(format!("board:{}", input.idempotency_key)),
            idempotency_key: IdempotencyKey(format!("board:{}", input.idempotency_key)),
            reason: "explicit authenticated local board action".into(),
            kind,
        },
    };
    match tokio::time::timeout(Duration::from_secs(3), state.service.handle(request)).await {
        Ok(Ok(_)) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(error)) => match error.code {
            ErrorCode::Conflict | ErrorCode::InvariantViolation => StatusCode::CONFLICT,
            ErrorCode::Validation | ErrorCode::UnsupportedProtocol => StatusCode::BAD_REQUEST,
            ErrorCode::Unavailable | ErrorCode::Internal => StatusCode::SERVICE_UNAVAILABLE,
        }
        .into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
