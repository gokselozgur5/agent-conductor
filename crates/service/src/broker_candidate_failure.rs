//! Retain failed owned-run evidence without exposing task output in diagnostics.
use crate::BrokerVendorOutcome;
use agent_conductor_store::StoreError;
use std::fmt;

/// A rejected candidate is never a task verdict. An outcome exists only when
/// supervision returned one; early refusal must not manufacture process evidence.
pub struct BrokerCandidateFailure {
    cause: StoreError,
    outcome: Option<BrokerVendorOutcome>,
}
impl BrokerCandidateFailure {
    /// Record refusal before supervision produced any outcome.
    #[must_use]
    pub fn before_outcome(cause: StoreError) -> Self {
        Self {
            cause,
            outcome: None,
        }
    }
    /// Retain caller-supplied failure observations, without authenticating them
    /// or granting publication authority. Only successful candidates are sealed.
    #[must_use]
    pub fn with_outcome(cause: StoreError, outcome: BrokerVendorOutcome) -> Self {
        Self {
            cause,
            outcome: Some(outcome),
        }
    }
    #[must_use]
    pub fn cause(&self) -> &StoreError {
        &self.cause
    }
    /// Private evidence may contain task data. Never publish it as telemetry.
    #[must_use]
    pub fn outcome(&self) -> Option<&BrokerVendorOutcome> {
        self.outcome.as_ref()
    }
}
impl fmt::Debug for BrokerCandidateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerCandidateFailure")
            .field("has_outcome", &self.outcome.is_some())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for BrokerCandidateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("broker candidate refused; inspect private failure evidence")
    }
}
impl std::error::Error for BrokerCandidateFailure {}

#[cfg(test)]
mod tests {
    use super::*;
    use conductor_runtime::{BrokerError, ProcessOutput};

    #[test]
    fn failed_candidate_preserves_private_output_without_formatting_it() {
        let failure = BrokerCandidateFailure::with_outcome(
            StoreError::Integrity("private-cause-canary".into()),
            BrokerVendorOutcome {
                process: Ok(ProcessOutput {
                    exit_code: Some(7),
                    stdout: b"private-stdout-canary".to_vec(),
                    stderr: b"private-stderr-canary".to_vec(),
                }),
                broker: Err(BrokerError::Invalid),
            },
        );
        let outcome = failure.outcome().unwrap();
        let output = outcome.process.as_ref().unwrap();
        assert_eq!(output.exit_code, Some(7));
        assert_eq!(output.stdout, b"private-stdout-canary");
        assert_eq!(output.stderr, b"private-stderr-canary");
        assert!(matches!(outcome.broker, Err(BrokerError::Invalid)));
        assert!(
            matches!(failure.cause(), StoreError::Integrity(value) if value == "private-cause-canary")
        );
        for rendered in [format!("{failure}"), format!("{failure:?}")] {
            assert!(!rendered.contains("canary"));
        }
        let early = BrokerCandidateFailure::before_outcome(StoreError::Integrity("early".into()));
        assert!(early.outcome().is_none());
    }
}
