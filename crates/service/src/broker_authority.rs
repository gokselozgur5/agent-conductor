//! Scoped bridge from service task authority to an already owned broker vendor.
use crate::{BrokerVendorOutcome, Clock, NativeBrokerSession, PersistentControlService};
use agent_conductor_store::{EffectAudit, FileBrokerBinding, StoreError};
use conductor_runtime::{PreparedBrokerVendor, ResumePacket};
use std::{future::Future, time::Duration};
use tokio::sync::watch;

impl<C: Clock> PersistentControlService<C> {
    /// Supervise an already released broker/vendor using current service ownership.
    /// This does not admit a native adapter or fence/complete its execution. The
    /// caller must establish OS/vendor admission before constructing this session.
    /// Dropping the future drops the vendor and broker owners; no task is detached.
    /// # Errors
    /// Refuses stale authority, mismatched contracts or failed command issuance.
    pub async fn run_broker_vendor(
        &self,
        session: NativeBrokerSession,
        prepared: PreparedBrokerVendor,
        event: EffectAudit<'_>,
    ) -> Result<BrokerVendorOutcome, StoreError> {
        let packet = prepared.packet().clone();
        let (binding, ceiling) = session.authority_binding();
        self.with_broker_authority(&packet, &binding, ceiling, |lease| {
            session.run_vendor(prepared, lease, event)
        })
        .await?
        .map_err(|_| refused())
    }

    pub(crate) async fn with_broker_authority<T, F, Fut>(
        &self,
        packet: &ResumePacket,
        binding: &FileBrokerBinding,
        ceiling: u64,
        operation: F,
    ) -> Result<T, StoreError>
    where
        F: FnOnce(watch::Receiver<Option<u64>>) -> Fut,
        Fut: Future<Output = T>,
    {
        let refresh = || async {
            tokio::time::timeout(Duration::from_millis(200), async {
                let _guard = self.mutations.lock().await;
                let (authority, generation) = self.store.runtime_authority().await?;
                if authority.as_deref() != Some(binding.authority_id.as_str())
                    || i64::try_from(generation).ok() != Some(binding.boot_generation)
                    || generation != self.boot_generation
                    || packet.contract.task_id != binding.task_id
                    || packet.contract.attempt_id != binding.attempt_id
                    || i64::try_from(packet.contract.lease_epoch).ok() != Some(binding.lease_epoch)
                {
                    return Err(refused());
                }
                let deadline = self.validate_runtime_owner(packet).await?.min(ceiling);
                if deadline <= conductor_runtime::continuous_millis().map_err(|_| refused())? {
                    return Err(refused());
                }
                Ok(deadline)
            })
            .await
            .map_err(|_| refused())?
        };
        let (authority, lease) = watch::channel(Some(refresh().await?));
        let operation = operation(lease);
        tokio::pin!(operation);
        let monitor = async {
            let mut tick = tokio::time::interval(Duration::from_millis(200));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let deadline = refresh().await.ok();
                authority.send_replace(deadline);
                if deadline.is_none() {
                    return;
                }
            }
        };
        tokio::select! {
            result = &mut operation => Ok(result),
            () = monitor => Ok(operation.await),
        }
    }
}

fn refused() -> StoreError {
    StoreError::Integrity("broker service authority is not current".into())
}
