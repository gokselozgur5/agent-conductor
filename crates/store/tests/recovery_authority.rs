use agent_conductor_store::Store;

#[tokio::test]
async fn reserved_boot_binds_identity_and_refuses_unreserved_or_stale_starts() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    let identity = "a".repeat(64);
    assert_eq!(
        store
            .start_reserved_runtime(1, &identity, 8)
            .await
            .unwrap()
            .boot_generation,
        8
    );
    assert!(store.start_runtime(2).await.is_err());
    assert!(store.start_reserved_runtime(2, &identity, 8).await.is_err());
    assert!(
        store
            .start_reserved_runtime(2, &"b".repeat(64), 9)
            .await
            .is_err()
    );
    assert_eq!(
        store.runtime_authority().await.unwrap(),
        (Some(identity.clone()), 8)
    );
    assert_eq!(
        store
            .start_reserved_runtime(3, &identity, 12)
            .await
            .unwrap()
            .boot_generation,
        12
    );
}

#[tokio::test]
async fn failed_first_reservation_rolls_back_binding_and_generation() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    store.start_runtime(1).await.unwrap();
    assert!(
        store
            .start_reserved_runtime(2, &"a".repeat(64), 1)
            .await
            .is_err()
    );
    assert_eq!(store.runtime_authority().await.unwrap(), (None, 1));
    for value in [0, u64::MAX] {
        assert!(
            store
                .start_reserved_runtime(2, &"a".repeat(64), value)
                .await
                .is_err()
        );
    }
    assert!(store.start_reserved_runtime(2, "invalid", 2).await.is_err());
    assert_eq!(store.runtime_authority().await.unwrap(), (None, 1));
}

#[tokio::test]
async fn reservation_at_integer_limit_is_exact_and_cannot_wrap() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    let identity = "a".repeat(64);
    let generation = i64::MAX.unsigned_abs();
    assert_eq!(
        store
            .start_reserved_runtime(1, &identity, generation)
            .await
            .unwrap()
            .boot_generation,
        generation
    );
    assert!(
        store
            .start_reserved_runtime(2, &identity, generation)
            .await
            .is_err()
    );
    assert_eq!(
        store.runtime_authority().await.unwrap(),
        (Some(identity), generation)
    );
}
