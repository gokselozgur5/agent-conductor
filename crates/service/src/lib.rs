use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use agent_conductor_store::{RuntimeInvariantViolation, Store, StoreError};
use conductor_protocol::{
    ErrorBody, HealthResponse, HealthStatus, InvariantResponse, InvariantViolation, Request,
    Response,
};

pub trait ControlService: Send + Sync + 'static {
    /// Handles one already validated control-plane request.
    ///
    /// # Errors
    ///
    /// Returns a structured service error when the requested operation cannot
    /// be completed without violating its contract.
    fn handle(&self, request: Request) -> impl Future<Output = Result<Response, ErrorBody>> + Send;
}

#[derive(Clone)]
pub struct PersistentControlService {
    store: Store,
    boot_generation: u64,
    started_at: Instant,
}

impl PersistentControlService {
    /// Performs the durable runtime-start transaction before exposing service readiness.
    ///
    /// # Errors
    ///
    /// Refuses construction when database integrity checks or boot-generation
    /// persistence fail.
    pub async fn start(store: Store, started_at_ms: i64) -> Result<Self, StoreError> {
        let startup = store.start_runtime(started_at_ms).await?;
        Ok(Self {
            store,
            boot_generation: startup.boot_generation,
            started_at: Instant::now(),
        })
    }

    async fn invariant_snapshot(&self) -> Result<Vec<InvariantViolation>, ErrorBody> {
        self.store
            .check_runtime_invariants()
            .await
            .map(|violations| violations.into_iter().map(map_violation).collect())
            .map_err(|error| internal_store_error(&error))
    }
}

impl ControlService for PersistentControlService {
    async fn handle(&self, request: Request) -> Result<Response, ErrorBody> {
        let violations = self.invariant_snapshot().await?;
        Ok(response_for(
            &request,
            self.boot_generation,
            self.started_at,
            violations,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryControlService {
    boot_generation: u64,
    started_at: Instant,
    violations: Arc<RwLock<Vec<InvariantViolation>>>,
}

impl InMemoryControlService {
    #[must_use]
    pub fn new(boot_generation: u64) -> Self {
        Self {
            boot_generation,
            started_at: Instant::now(),
            violations: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Replaces the mock invariant set used by the Phase 1 runtime shell.
    ///
    /// # Panics
    ///
    /// Panics only if another thread poisoned the in-memory test lock.
    pub fn replace_violations(&self, violations: Vec<InvariantViolation>) {
        *self.violations.write().expect("invariant lock is healthy") = violations;
    }

    fn invariant_snapshot(&self) -> Vec<InvariantViolation> {
        self.violations
            .read()
            .expect("invariant lock is healthy")
            .clone()
    }
}

impl ControlService for InMemoryControlService {
    async fn handle(&self, request: Request) -> Result<Response, ErrorBody> {
        let violations = self.invariant_snapshot();
        let boot_generation = self.boot_generation;
        let started_at = self.started_at;
        Ok(response_for(
            &request,
            boot_generation,
            started_at,
            violations,
        ))
    }
}

fn response_for(
    request: &Request,
    boot_generation: u64,
    started_at: Instant,
    violations: Vec<InvariantViolation>,
) -> Response {
    match request {
        Request::Health => Response::Health(HealthResponse {
            status: if violations.is_empty() {
                HealthStatus::Ready
            } else {
                HealthStatus::Degraded
            },
            boot_generation,
            uptime_millis: u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        }),
        Request::CheckInvariants => Response::Invariants(InvariantResponse {
            healthy: violations.is_empty(),
            violations,
        }),
    }
}

fn map_violation(violation: RuntimeInvariantViolation) -> InvariantViolation {
    let context = serde_json::from_str(&violation.context_json)
        .unwrap_or(serde_json::Value::String(violation.context_json));
    InvariantViolation {
        code: violation.code,
        summary: violation.summary,
        context,
    }
}

fn internal_store_error(error: &StoreError) -> ErrorBody {
    ErrorBody::new(
        conductor_protocol::ErrorCode::Internal,
        format!("persistent state unavailable: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use conductor_protocol::{HealthStatus, InvariantViolation};
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn reports_degraded_health_when_an_invariant_is_violated() {
        let service = InMemoryControlService::new(7);
        service.replace_violations(vec![InvariantViolation {
            code: "duplicate_lease".to_owned(),
            summary: "two active leases".to_owned(),
            context: json!({"task_id": "task-1"}),
        }]);

        let Response::Health(health) = service
            .handle(Request::Health)
            .await
            .expect("health responds")
        else {
            panic!("unexpected response");
        };
        assert_eq!(health.status, HealthStatus::Degraded);
        assert_eq!(health.boot_generation, 7);

        let Response::Invariants(invariants) = service
            .handle(Request::CheckInvariants)
            .await
            .expect("invariants respond")
        else {
            panic!("unexpected response");
        };
        assert!(!invariants.healthy);
        assert_eq!(invariants.violations.len(), 1);
    }

    #[tokio::test]
    async fn persistent_health_uses_the_durable_boot_generation() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = directory.path().join("conductor.sqlite3");

        let first = PersistentControlService::start(
            Store::open(&database).await.expect("first store opens"),
            100,
        )
        .await
        .expect("first runtime starts");
        let Response::Health(first_health) = first
            .handle(Request::Health)
            .await
            .expect("first health responds")
        else {
            panic!("unexpected response");
        };
        assert_eq!(first_health.status, HealthStatus::Ready);
        assert_eq!(first_health.boot_generation, 1);
        drop(first);

        let second = PersistentControlService::start(
            Store::open(&database).await.expect("second store opens"),
            200,
        )
        .await
        .expect("second runtime starts");
        let Response::Health(second_health) = second
            .handle(Request::Health)
            .await
            .expect("second health responds")
        else {
            panic!("unexpected response");
        };
        assert_eq!(second_health.status, HealthStatus::Ready);
        assert_eq!(second_health.boot_generation, 2);
    }
}
