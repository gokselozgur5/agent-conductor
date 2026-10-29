//! Joint ownership of a vendor process and its revocable file broker.
use crate::NativeBrokerSession;
use agent_conductor_store::EffectAudit;
use conductor_runtime::{
    BrokerError, BrokerShutdownReceipt, PreparedBrokerVendor, ProcessError, ProcessOutput,
    ProcessSpec, VendorError, continuous_millis, run_process,
};
use std::time::Duration;
use tokio::sync::watch;

/// Process completion and durable broker closure are separate observations.
/// Neither establishes vendor confinement, execution fencing or task acceptance.
pub struct BrokerVendorOutcome {
    pub process: Result<ProcessOutput, ProcessError>,
    pub broker: Result<BrokerShutdownReceipt, BrokerError>,
}

impl NativeBrokerSession {
    /// Run the exact prepared vendor while owning its broker for the entire future.
    /// The caller must enforce OS/vendor admission and executable immutability.
    /// `lease` carries deadlines from live lease authority, never inferred extensions;
    /// None, sender loss, expiration or failed renewal stops the controlled process.
    /// Dropping this future drops both owners and leaves durable recovery evidence.
    /// # Errors
    /// Refuses command issuance if its frozen packet no longer matches this session.
    /// Process errors and broker settlement errors remain separate in the outcome.
    pub async fn run_vendor(
        mut self,
        prepared: PreparedBrokerVendor,
        lease: watch::Receiver<Option<u64>>,
        event: EffectAudit<'_>,
    ) -> Result<BrokerVendorOutcome, VendorError> {
        let spec = self.vendor_command(prepared).await?;
        Ok(self.supervise_vendor_spec(spec, lease, event).await)
    }

    // Private seam for controlled-process tests; public entry accepts only an
    // immutable prepared vendor bound by vendor_command to the frozen packet.
    pub(crate) async fn supervise_vendor_spec(
        mut self,
        spec: ProcessSpec,
        mut lease: watch::Receiver<Option<u64>>,
        event: EffectAudit<'_>,
    ) -> BrokerVendorOutcome {
        let initial = *lease.borrow_and_update();
        let live = match initial {
            Some(deadline) if lease.has_changed().is_ok() => self.renew(deadline).await.is_ok(),
            _ => false,
        };
        let process = if live {
            let (cancel, cancelled) = watch::channel(false);
            let process = run_process(&spec, cancelled);
            tokio::pin!(process);
            let mut heartbeat = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {
                    result = &mut process => break result,
                    _ = lease.changed() => {},
                    _ = heartbeat.tick() => {},
                }
                let deadline = *lease.borrow_and_update();
                let valid = match (deadline, continuous_millis()) {
                    (Some(deadline), Ok(now)) if deadline > now && lease.has_changed().is_ok() => {
                        self.renew(deadline).await.is_ok()
                    }
                    _ => false,
                };
                if !valid {
                    // Cancel both capabilities before observing process completion.
                    // The private receipt, not cancellation delivery, proves closure.
                    let _ = cancel.send(true);
                    let _ = self.cancel().await;
                    break process.await;
                }
            }
        } else {
            Err(ProcessError::Cancelled)
        };
        // Normal proxy EOF may already have stopped the broker. Always require
        // independent finish evidence; cancellation failure alone is inconclusive.
        let _ = self.cancel().await;
        let broker = self.finish(event).await;
        BrokerVendorOutcome { process, broker }
    }
}
