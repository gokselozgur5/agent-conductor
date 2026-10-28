use agent_conductor_domain::{Command, RequiredCheck, SourceIdentity, Task};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

pub const PROTOCOL_NAME: &str = "agent-conductor.local";
pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestEnvelope {
    pub protocol: String,
    pub version: u16,
    pub message_id: Uuid,
    pub correlation_id: Uuid,
    pub payload: Request,
}

impl RequestEnvelope {
    #[must_use]
    pub fn new(payload: Request) -> Self {
        Self {
            protocol: PROTOCOL_NAME.to_owned(),
            version: PROTOCOL_VERSION,
            message_id: Uuid::now_v7(),
            correlation_id: Uuid::now_v7(),
            payload,
        }
    }

    /// Validates the transport contract before a request reaches the service.
    ///
    /// # Errors
    ///
    /// Returns a protocol error for an unsupported protocol name or version.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.protocol != PROTOCOL_NAME {
            return Err(ProtocolError::UnsupportedProtocol(self.protocol.clone()));
        }
        if self.version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion(self.version));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Inventory {
        resource: InventoryResource,
        after: Option<String>,
    },
    Health,
    CheckInvariants,
    SubmitWorkflow {
        plan_json: String,
    },
    ListWorkflows,
    ReassignWorkflow {
        task_id: String,
        expected_version: u64,
        worker_id: String,
        pin_json: String,
    },
    ContinueWorkflow {
        task_id: String,
        expected_version: u64,
    },
    RegisterProject {
        actor_id: String,
        idempotency_key: String,
        id: String,
        path: String,
    },
    RegisterAgent {
        actor_id: String,
        idempotency_key: String,
        id: String,
        adapter_kind: String,
        protocol_fingerprint: String,
        capability_hash: String,
        access_mode: String,
    },
    CreateTask {
        actor_id: String,
        idempotency_key: String,
        id: String,
        project_id: String,
        objective: String,
        priority: i64,
        authority: Value,
        required_checks: Vec<RequiredCheck>,
    },
    GetTask {
        task_id: String,
    },
    ListTasks {
        project_id: Option<String>,
    },
    ListTaskPage {
        project_id: Option<String>,
        after: Option<String>,
    },
    TaskCheckpoint {
        task_id: String,
    },
    TaskEvidence {
        task_id: String,
        before: Option<String>,
    },
    TaskHistory {
        task_id: String,
        before: Option<String>,
    },
    ApplyCommand {
        task_id: String,
        command: Command,
    },
    /// Privileged conductor operation. The daemon supplies the System actor;
    /// callers cannot assert that role through `ApplyCommand`.
    BindVerificationSource {
        task_id: String,
        expected_version: u64,
        idempotency_key: String,
        source: SourceIdentity,
    },
    SubscribeEvents {
        subscription: EventSubscriptionInput,
    },
    PublishEvent {
        event: SemanticEventInput,
    },
    ClaimEvent {
        agent_id: String,
    },
    AcknowledgeEvent {
        delivery_id: String,
        consumer_agent_id: String,
        claim_token: String,
        effect: ConsumerStateInput,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    pub protocol: String,
    pub version: u16,
    pub message_id: Uuid,
    pub correlation_id: Uuid,
    pub request_message_id: Option<Uuid>,
    pub result: ResponseResult,
}

impl ResponseEnvelope {
    #[must_use]
    pub fn success(request: &RequestEnvelope, response: Response) -> Self {
        Self {
            protocol: PROTOCOL_NAME.to_owned(),
            version: PROTOCOL_VERSION,
            message_id: Uuid::now_v7(),
            correlation_id: request.correlation_id,
            request_message_id: Some(request.message_id),
            result: ResponseResult::Ok(response),
        }
    }

    #[must_use]
    pub fn failure(request: Option<&RequestEnvelope>, error: ErrorBody) -> Self {
        Self {
            protocol: PROTOCOL_NAME.to_owned(),
            version: PROTOCOL_VERSION,
            message_id: Uuid::now_v7(),
            correlation_id: request.map_or_else(Uuid::now_v7, |value| value.correlation_id),
            request_message_id: request.map(|value| value.message_id),
            result: ResponseResult::Error(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "payload", rename_all = "snake_case")]
pub enum ResponseResult {
    Ok(Response),
    Error(ErrorBody),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Inventory {
        resource: InventoryResource,
        entries: Vec<Value>,
        next_after: Option<String>,
    },
    WorkflowQueued {
        task_id: String,
    },
    Workflows {
        entries: Vec<Value>,
    },
    Health(HealthResponse),
    Invariants(InvariantResponse),
    ProjectRegistered {
        id: String,
    },
    AgentRegistered {
        id: String,
    },
    Task(TaskResponse),
    TaskCheckpoint {
        view: Value,
    },
    Tasks {
        tasks: Vec<TaskResponse>,
    },
    TaskEvidence {
        entries: Vec<TaskEvidenceEntry>,
        next_before: Option<String>,
    },
    TaskHistory {
        entries: Vec<TaskHistoryEntry>,
        next_before: Option<String>,
    },
    TaskPage {
        tasks: Vec<TaskResponse>,
        next_after: Option<String>,
    },
    CommandApplied(CommandAppliedResponse),
    EventSubscribed {
        subscription_id: String,
    },
    EventPublished(EventPublishedResponse),
    EventDelivery {
        delivery: Option<Box<EventDeliveryResponse>>,
    },
    EventAcknowledged {
        delivery_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventSubscriptionInput {
    pub schema_version: u16,
    pub id: String,
    pub agent_id: String,
    pub topic_filter: String,
    pub project_id: Option<String>,
    pub path_prefix: Option<String>,
    #[serde(default)]
    pub echo_source: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticEventInput {
    pub schema_version: u16,
    pub id: String,
    pub idempotency_key: Option<String>,
    pub source_agent_id: String,
    pub source_session_id: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub generation: u32,
    pub topic: String,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub attempt_id: Option<String>,
    pub lease_epoch: Option<u64>,
    pub path_scope: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerStateInput {
    pub namespace: String,
    pub key: String,
    pub expected_version: Option<u64>,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventPublishedResponse {
    pub event_id: String,
    pub fanout: usize,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventDeliveryResponse {
    pub schema_version: u16,
    pub delivery_id: String,
    pub event: SemanticEventResponse,
    pub attempts: u32,
    pub claim_token: String,
    pub lease_expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticEventResponse {
    pub schema_version: u16,
    pub id: String,
    pub idempotency_key: String,
    pub source_agent_id: String,
    pub source_session_id: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub generation: u32,
    pub topic: String,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub attempt_id: Option<String>,
    pub lease_epoch: Option<u64>,
    pub path_scope: Option<String>,
    pub payload: Value,
    pub created_at_ms: i64,
}

/// Metadata only; raw audit commands and payloads never cross this projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskHistoryEntry {
    pub sequence: String,
    pub kind: String,
    pub actor_id: String,
    pub correlation_id: String,
    pub occurred_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEvidenceEntry {
    pub sequence: String,
    pub kind: String,
    pub actor_id: String,
    pub occurred_at_ms: i64,
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskResponse {
    pub project_id: String,
    pub objective: String,
    pub priority: i64,
    pub authority: Value,
    pub aggregate: Box<Task>,
    /// Present only on fresh task reads with an attempt, never historical outcomes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_observation: Option<LeaseObservation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandAppliedResponse {
    pub task: TaskResponse,
    pub replayed: bool,
    pub effect_tokens: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    #[serde(default)]
    pub repository_watcher: WatcherStatus,
    pub status: HealthStatus,
    pub boot_generation: u64,
    pub uptime_millis: u64,
}

/// Process-local observation health; disabled means observation was not requested.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatcherStatus {
    #[default]
    Disabled,
    Starting,
    Running,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Ready,
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvariantResponse {
    pub healthy: bool,
    pub violations: Vec<InvariantViolation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvariantViolation {
    pub code: String,
    pub summary: String,
    pub context: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
    pub details: Option<Value>,
}

impl ErrorBody {
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Validation,
    Conflict,
    Unavailable,
    InvariantViolation,
    UnsupportedProtocol,
    Internal,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("unsupported protocol: {0}")]
    UnsupportedProtocol(String),
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u16),
}

impl From<ProtocolError> for ErrorBody {
    fn from(value: ProtocolError) -> Self {
        Self::new(ErrorCode::UnsupportedProtocol, value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trip_preserves_identifiers() {
        let request = RequestEnvelope::new(Request::Health);
        let json = serde_json::to_string(&request).expect("request serializes");
        let decoded: RequestEnvelope = serde_json::from_str(&json).expect("request parses");

        assert_eq!(decoded, request);
        assert_eq!(decoded.validate(), Ok(()));
    }

    #[test]
    fn missing_required_field_is_rejected() {
        let error = serde_json::from_str::<RequestEnvelope>(
            r#"{"protocol":"agent-conductor.local","version":1,"payload":{"type":"health"}}"#,
        )
        .expect_err("message identifiers are required");

        assert!(error.to_string().contains("message_id"));
    }

    #[test]
    fn unknown_optional_envelope_fields_are_ignored() {
        let request = RequestEnvelope::new(Request::CheckInvariants);
        let mut value = serde_json::to_value(&request).expect("request converts to JSON");
        value
            .as_object_mut()
            .expect("envelope is an object")
            .insert("future_optional".to_owned(), Value::Bool(true));

        let decoded: RequestEnvelope =
            serde_json::from_value(value).expect("unknown field is safe");
        assert_eq!(decoded, request);
    }

    #[test]
    fn unsupported_major_version_fails_closed() {
        let mut request = RequestEnvelope::new(Request::Health);
        request.version = PROTOCOL_VERSION + 1;

        assert_eq!(
            request.validate(),
            Err(ProtocolError::UnsupportedVersion(PROTOCOL_VERSION + 1))
        );
    }

    #[test]
    fn caller_cannot_supply_project_identity_fields() {
        let value = serde_json::json!({
            "type": "register_project",
            "actor_id": "operator",
            "idempotency_key": "project-1",
            "id": "project-1",
            "path": "/workspace/project",
            "filesystem_identity": "spoofed"
        });
        let error = serde_json::from_value::<Request>(value)
            .expect_err("caller-supplied identity must be rejected");
        assert!(error.to_string().contains("unknown field"));
    }

    fn sample_task_response() -> TaskResponse {
        TaskResponse {
            project_id: "project-1".into(),
            objective: "prove list framing".into(),
            priority: 7,
            authority: serde_json::json!({"mode": "test"}),
            aggregate: Box::new(Task::new("task-1".into(), vec![])),
            lease_observation: None,
        }
    }

    #[test]
    fn every_response_variant_round_trips() {
        let task = sample_task_response();
        let event = SemanticEventResponse {
            schema_version: 1,
            id: "event-1".into(),
            idempotency_key: "key-1".into(),
            source_agent_id: "agent-1".into(),
            source_session_id: "session-1".into(),
            correlation_id: "correlation-1".into(),
            causation_id: None,
            generation: 0,
            topic: "task.changed".into(),
            project_id: Some("project-1".into()),
            task_id: Some("task-1".into()),
            attempt_id: Some("attempt-1".into()),
            lease_epoch: Some(1),
            path_scope: None,
            payload: serde_json::json!({"state": "ready"}),
            created_at_ms: 1,
        };
        let responses = vec![
            Response::Health(HealthResponse {
                repository_watcher: WatcherStatus::Disabled,
                status: HealthStatus::Ready,
                boot_generation: 1,
                uptime_millis: 2,
            }),
            Response::Invariants(InvariantResponse {
                healthy: true,
                violations: vec![],
            }),
            Response::ProjectRegistered { id: "p".into() },
            Response::AgentRegistered { id: "a".into() },
            Response::Task(task.clone()),
            Response::Tasks { tasks: vec![] },
            Response::Tasks {
                tasks: vec![task.clone()],
            },
            Response::CommandApplied(CommandAppliedResponse {
                task,
                replayed: false,
                effect_tokens: vec!["effect-1".into()],
            }),
            Response::EventSubscribed {
                subscription_id: "subscription-1".into(),
            },
            Response::EventPublished(EventPublishedResponse {
                event_id: "event-1".into(),
                fanout: 1,
                duplicate: false,
            }),
            Response::EventDelivery { delivery: None },
            Response::EventDelivery {
                delivery: Some(Box::new(EventDeliveryResponse {
                    schema_version: 1,
                    delivery_id: "delivery-1".into(),
                    event,
                    attempts: 1,
                    claim_token: "claim-1".into(),
                    lease_expires_at_ms: 10,
                })),
            },
            Response::EventAcknowledged {
                delivery_id: "delivery-1".into(),
            },
        ];
        for response in responses {
            let json = serde_json::to_string(&response).expect("response serializes");
            let decoded: Response = serde_json::from_str(&json).expect("response parses");
            assert_eq!(decoded, response);
        }
    }
}

/// Bounded, read-only operator metadata collections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InventoryResource {
    Projects,
    Agents,
    Events,
    Subscriptions,
    Deliveries,
    Usage,
}

/// A read-time domain lease observation, not OS process liveness or permission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseObservation {
    pub observed_at_ms: i64,
    pub daemon_boot_generation: u64,
    pub boot_matches: bool,
    pub accepts_worker_mutation: bool,
    pub worker_remaining_ms: Option<u64>,
    pub owner_remaining_ms: Option<u64>,
}
