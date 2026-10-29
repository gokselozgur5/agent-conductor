#[tokio::test]
async fn broker_service_authority_latches_expiry() {
    let (directory, store) = lifecycle_store().await;
    let clock = FixedClock::new(0);
    let service = PersistentControlService::start_with_clock(store.clone(), 100, clock.clone())
        .await
        .unwrap();
    claim_task(&service).await;
    let packet = runtime_launch(directory.path()).packet;
    let binding = broker_authority_binding(directory.path(), &store).await;
    let ceiling = u64::MAX;
    service
        .with_broker_authority(&packet, &binding, ceiling, |mut lease| async move {
            assert!(
                lease
                    .borrow_and_update()
                    .is_some_and(|deadline| deadline < ceiling)
            );
            clock.set(10);
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while lease.borrow_and_update().is_some() {
                    lease.changed().await.unwrap();
                }
            })
            .await
            .unwrap();
            // A later clock rollback cannot revive the revoked grant.
            clock.set(0);
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            assert_eq!(*lease.borrow(), None);
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn broker_service_authority_rejects_stale_scope_before_polling_vendor() {
    let (directory, store) = lifecycle_store().await;
    let service =
        PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
            .await
            .unwrap();
    claim_task(&service).await;
    let packet = runtime_launch(directory.path()).packet;
    let binding = broker_authority_binding(directory.path(), &store).await;
    for mode in 0..5 {
        let mut changed = binding.clone();
        match mode {
            0 => changed.authority_id = "b".repeat(64),
            1 => changed.boot_generation += 1,
            2 => changed.task_id = "other".into(),
            3 => changed.attempt_id = "other".into(),
            4 => changed.lease_epoch += 1,
            _ => unreachable!(),
        }
        let result = service
            .with_broker_authority(&packet, &changed, u64::MAX, |_| async {
                panic!("stale authority must not poll the vendor")
            })
            .await;
        assert!(result.is_err());
    }
}

async fn broker_authority_binding(
    directory: &Path,
    store: &Store,
) -> agent_conductor_store::FileBrokerBinding {
    let mut db = <sqlx::SqliteConnection as sqlx::Connection>::connect(&format!(
        "sqlite:{}",
        directory.join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    // Controlled lifecycle fixtures do not reserve a production host authority.
    sqlx::query("UPDATE recovery_authority SET authority_id=?")
        .bind("a".repeat(64))
        .execute(&mut db)
        .await
        .unwrap();
    let (authority_id, generation) = store.runtime_authority().await.unwrap();
    agent_conductor_store::FileBrokerBinding {
        authority_id: authority_id.unwrap(),
        boot_generation: i64::try_from(generation).unwrap(),
        invocation_id: Uuid::now_v7().to_string(),
        execution_token: "fixture".into(),
        task_id: "task".into(),
        attempt_id: "attempt".into(),
        lease_epoch: 1,
        root_identity_sha256: "a".repeat(64),
    }
}
