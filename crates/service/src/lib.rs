use std::{
    future::Future,
    sync::{Arc, RwLock},
    time::Instant,
};

use agent_conductor_store::{RuntimeInvariantViolation, Store, StoreError};
use conductor_protocol::{
    ErrorBody, ErrorCode, HealthResponse, HealthStatus, InvariantResponse, InvariantViolation,
    Request, Response,
};

pub trait ControlService: Send + Sync + 'static {
    fn handle(&self, request: Request) -> impl Future<Output = Result<Response, ErrorBody>> + Send;
}

#[derive(Clone)]
pub struct PersistentControlService {
    store: Store,
    boot_generation: u64,
    started_at: Instant,
}

impl PersistentControlService {
    /// Starts the persistent runtime after durable startup checks.
    ///
    /// # Errors
    ///
    /// Returns a store error when integrity checks or generation advancement fail.
    pub async fn start(store: Store, started_at_ms: i64) -> Result<Self, StoreError> {
        let startup = store.start_runtime(started_at_ms).await?;
        Ok(Self {
            store,
            boot_generation: startup.boot_generation,
            started_at: Instant::now(),
        })
    }

    async fn violations(&self) -> Result<Vec<InvariantViolation>, ErrorBody> {
        self.store
            .check_runtime_invariants()
            .await
            .map(|items| items.into_iter().map(map_violation).collect())
            .map_err(|error| {
                ErrorBody::new(
                    ErrorCode::Internal,
                    format!("persistent state unavailable: {error}"),
                )
            })
    }
}

impl ControlService for PersistentControlService {
    async fn handle(&self, request: Request) -> Result<Response, ErrorBody> {
        let violations = self.violations().await?;
        response_for(&request, self.boot_generation, self.started_at, violations)
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
    /// Replaces test-only invariant violations.
    ///
    /// # Panics
    ///
    /// Panics if a prior test thread poisoned the lock.
    pub fn replace_violations(&self, violations: Vec<InvariantViolation>) {
        *self.violations.write().expect("invariant lock is healthy") = violations;
    }
}

impl ControlService for InMemoryControlService {
    async fn handle(&self, request: Request) -> Result<Response, ErrorBody> {
        let violations = self
            .violations
            .read()
            .expect("invariant lock is healthy")
            .clone();
        response_for(&request, self.boot_generation, self.started_at, violations)
    }
}

fn response_for(
    request: &Request,
    boot_generation: u64,
    started_at: Instant,
    violations: Vec<InvariantViolation>,
) -> Result<Response, ErrorBody> {
    match request {
        Request::Health => Ok(Response::Health(HealthResponse {
            status: if violations.is_empty() {
                HealthStatus::Ready
            } else {
                HealthStatus::Degraded
            },
            boot_generation,
            uptime_millis: u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        })),
        Request::CheckInvariants => Ok(Response::Invariants(InvariantResponse {
            healthy: violations.is_empty(),
            violations,
        })),
        _ => Err(ErrorBody::new(
            ErrorCode::Unavailable,
            "persistent mutation service is not wired",
        )),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn persistent_health_uses_durable_generation() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = directory.path().join("conductor.sqlite3");
        let first =
            PersistentControlService::start(Store::open(&database).await.expect("open"), 100)
                .await
                .expect("start");
        let Response::Health(first) = first.handle(Request::Health).await.expect("health") else {
            panic!("unexpected response")
        };
        assert_eq!(first.boot_generation, 1);
        let second =
            PersistentControlService::start(Store::open(&database).await.expect("reopen"), 200)
                .await
                .expect("restart");
        let Response::Health(second) = second.handle(Request::Health).await.expect("health") else {
            panic!("unexpected response")
        };
        assert_eq!(second.boot_generation, 2);
    }
}
