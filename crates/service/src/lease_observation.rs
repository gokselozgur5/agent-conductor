use agent_conductor_domain::{ClockReading, LeaseStatus, Task};
use conductor_protocol::LeaseObservation;

pub(super) fn observe(task: &Task, clock: ClockReading) -> Option<LeaseObservation> {
    let lease = &task.attempt.as_ref()?.lease;
    let boot_matches = lease.boot_generation == clock.boot_generation;
    let has_current_deadlines = boot_matches && lease.status != LeaseStatus::Ended;
    Some(LeaseObservation {
        observed_at_ms: clock.wall_unix_ms,
        daemon_boot_generation: clock.boot_generation,
        boot_matches,
        accepts_worker_mutation: lease.accepts_worker_mutation(clock),
        worker_remaining_ms: has_current_deadlines.then(|| {
            lease
                .worker_deadline_monotonic_ms
                .saturating_sub(clock.monotonic_ms)
        }),
        owner_remaining_ms: has_current_deadlines.then(|| {
            lease
                .deadline_monotonic_ms
                .saturating_sub(clock.monotonic_ms)
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_conductor_domain::{Attempt, Lease};

    fn task() -> Task {
        let mut task = Task::new("task".into(), vec![]);
        task.attempt = Some(Attempt {
            id: "attempt".into(),
            worker: "worker".into(),
            lease: Lease {
                epoch: 4,
                boot_generation: 2,
                deadline_monotonic_ms: 1000,
                worker_deadline_monotonic_ms: 900,
                remaining_at_last_heartbeat_ms: 1000,
                recovery_grace_ms: 100,
                status: LeaseStatus::Active,
            },
        });
        task
    }
    fn clock(monotonic_ms: u64) -> ClockReading {
        ClockReading {
            boot_generation: 2,
            monotonic_ms,
            wall_unix_ms: 10_000,
        }
    }

    #[test]
    fn expiry_is_sampled_without_mutating_the_task_or_using_wall_time() {
        let task = task();
        let original = task.clone();
        let before = observe(&task, clock(899)).unwrap();
        assert!(before.accepts_worker_mutation);
        assert_eq!(before.worker_remaining_ms, Some(1));
        assert_eq!(before.owner_remaining_ms, Some(101));
        let boundary = observe(&task, clock(900)).unwrap();
        assert!(!boundary.accepts_worker_mutation);
        assert_eq!(boundary.worker_remaining_ms, Some(0));
        assert_eq!(boundary.owner_remaining_ms, Some(100));
        for wall_unix_ms in [i64::MIN, 0, i64::MAX] {
            let sample = observe(
                &task,
                ClockReading {
                    wall_unix_ms,
                    ..clock(899)
                },
            )
            .unwrap();
            assert!(sample.accepts_worker_mutation);
            assert_eq!(sample.worker_remaining_ms, Some(1));
        }
        let expired = observe(&task, clock(u64::MAX)).unwrap();
        assert!(!expired.accepts_worker_mutation);
        assert_eq!(expired.owner_remaining_ms, Some(0));
        assert_eq!(task, original);
    }

    #[test]
    fn other_boot_recovery_and_ended_leases_never_claim_worker_validity() {
        let mut task = task();
        let other = observe(
            &task,
            ClockReading {
                boot_generation: 3,
                ..clock(0)
            },
        )
        .unwrap();
        assert!(!other.boot_matches && !other.accepts_worker_mutation);
        assert_eq!(other.worker_remaining_ms, None);
        assert_eq!(other.owner_remaining_ms, None);
        task.attempt.as_mut().unwrap().lease.status = LeaseStatus::RecoveryPending;
        let recovery = observe(&task, clock(0)).unwrap();
        assert!(recovery.boot_matches);
        assert!(!recovery.accepts_worker_mutation);
        task.attempt.as_mut().unwrap().lease.status = LeaseStatus::Ended;
        let ended = observe(&task, clock(0)).unwrap();
        assert!(!ended.accepts_worker_mutation);
        assert_eq!(ended.worker_remaining_ms, None);
        assert_eq!(ended.owner_remaining_ms, None);
        task.attempt = None;
        assert_eq!(observe(&task, clock(0)), None);
    }

    #[test]
    fn historical_task_responses_decode_without_fresh_observations() {
        let historical = serde_json::json!({"project_id":"project", "objective":"task", "priority":0, "authority":{}, "aggregate":task()});
        let response: conductor_protocol::TaskResponse =
            serde_json::from_value(historical).unwrap();
        assert_eq!(response.lease_observation, None);
        assert!(
            serde_json::to_value(response)
                .unwrap()
                .get("lease_observation")
                .is_none()
        );
    }
}
