use std::sync::{Arc, RwLock};
use std::time::Instant;

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
    fn handle(&self, request: Request) -> Result<Response, ErrorBody>;
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
    fn handle(&self, request: Request) -> Result<Response, ErrorBody> {
        let violations = self.invariant_snapshot();
        match request {
            Request::Health => Ok(Response::Health(HealthResponse {
                status: if violations.is_empty() {
                    HealthStatus::Ready
                } else {
                    HealthStatus::Degraded
                },
                boot_generation: self.boot_generation,
                uptime_millis: u64::try_from(self.started_at.elapsed().as_millis())
                    .unwrap_or(u64::MAX),
            })),
            Request::CheckInvariants => Ok(Response::Invariants(InvariantResponse {
                healthy: violations.is_empty(),
                violations,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use conductor_protocol::{HealthStatus, InvariantViolation};
    use serde_json::json;

    use super::*;

    #[test]
    fn reports_degraded_health_when_an_invariant_is_violated() {
        let service = InMemoryControlService::new(7);
        service.replace_violations(vec![InvariantViolation {
            code: "duplicate_lease".to_owned(),
            summary: "two active leases".to_owned(),
            context: json!({"task_id": "task-1"}),
        }]);

        let Response::Health(health) = service.handle(Request::Health).expect("health responds")
        else {
            panic!("unexpected response");
        };
        assert_eq!(health.status, HealthStatus::Degraded);
        assert_eq!(health.boot_generation, 7);

        let Response::Invariants(invariants) = service
            .handle(Request::CheckInvariants)
            .expect("invariants respond")
        else {
            panic!("unexpected response");
        };
        assert!(!invariants.healthy);
        assert_eq!(invariants.violations.len(), 1);
    }
}
