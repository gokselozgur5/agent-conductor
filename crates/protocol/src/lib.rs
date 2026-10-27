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
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Health,
    CheckInvariants,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    pub protocol: String,
    pub version: u16,
    pub message_id: Uuid,
    pub correlation_id: Uuid,
    pub request_message_id: Option<Uuid>,
    pub result: Result<Response, ErrorBody>,
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
            result: Ok(response),
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
            result: Err(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Health(HealthResponse),
    Invariants(InvariantResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub boot_generation: u64,
    pub uptime_millis: u64,
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
}
