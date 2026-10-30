//! Process death at actual SQLite writes and after successful owner commits.
use super::*;
use std::path::Path;

const OWNERS: [Owner; 16] = [
    Owner::Project,
    Owner::Task,
    Owner::Persist,
    Owner::Sequence,
    Owner::Startup,
    Owner::Effect,
    Owner::Publish,
    Owner::Observation,
    Owner::Delivery,
    Owner::Ack,
    Owner::Enqueue,
    Owner::Dispatch,
    Owner::Prepare,
    Owner::Update,
    Owner::Reassign,
    Owner::Reconcile,
];
const CASE: &str = "CONDUCTOR_TRANSACTION_CRASH_CASE";
const CONTROL: &str = "CONDUCTOR_TRANSACTION_CRASH_CONTROL";

async fn child(owner: Owner, committed: bool, control: &Path) -> ! {
    let (root, store, delivery) = fixture(owner).await;
    std::fs::write(
        control.join("database"),
        root.path().join("state.sqlite3").to_str().unwrap(),
    )
    .unwrap();
    let before = snapshot(&mut *store.inner.writer.lock().await).await;
    std::fs::write(
        control.join("before.json"),
        serde_json::to_vec(&before).unwrap(),
    )
    .unwrap();
    if !committed {
        let (table, limit) = owner.barrier();
        let mut count = 0;
        store
            .inner
            .writer
            .lock()
            .await
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |update| {
                if update.table == table {
                    count += 1;
                    if count == limit {
                        // No unwinding or connection cleanup before process death.
                        std::process::exit(78);
                    }
                }
            });
    }
    run(&store, owner, delivery.as_ref()).await.unwrap();
    assert!(
        committed,
        "selected write barrier was not reached: {owner:?}"
    );
    let after = snapshot(&mut *store.inner.writer.lock().await).await;
    assert_ne!(before, after, "positive control did not mutate: {owner:?}");
    std::fs::write(
        control.join("after.json"),
        serde_json::to_vec(&after).unwrap(),
    )
    .unwrap();
    std::process::exit(79);
}

#[tokio::test]
async fn transaction_owners_survive_abrupt_process_exit() {
    if let Ok(case) = std::env::var(CASE) {
        let index: usize = case.parse().unwrap();
        let control = std::path::PathBuf::from(std::env::var(CONTROL).unwrap());
        child(OWNERS[index / 2], index % 2 == 1, &control).await;
    }
    for (index, owner) in OWNERS.iter().enumerate() {
        for committed in [false, true] {
            let control = tempfile::tempdir().unwrap();
            let mut process = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "transaction_cancellation::abrupt_process::transaction_owners_survive_abrupt_process_exit", "--nocapture"])
                .env(CASE, (index * 2 + usize::from(committed)).to_string())
                .env(CONTROL, control.path()).env("TMPDIR", control.path())
                .spawn().unwrap();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            let status = loop {
                if let Some(status) = process.try_wait().unwrap() {
                    break status;
                }
                if tokio::time::Instant::now() >= deadline {
                    process.kill().unwrap();
                    process.wait().unwrap();
                    panic!("process fixture timed out: {owner:?}, committed={committed}");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            assert_eq!(
                status.code(),
                Some(if committed { 79 } else { 78 }),
                "{owner:?}"
            );
            let path = std::fs::read_to_string(control.path().join("database")).unwrap();
            let store = Store::open(path).await.unwrap();
            let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
                .fetch_one(store.readers())
                .await
                .unwrap();
            assert_eq!(integrity, "ok", "{owner:?}");
            let expected: Vec<(String, String)> = serde_json::from_slice(
                &std::fs::read(control.path().join(if committed {
                    "after.json"
                } else {
                    "before.json"
                }))
                .unwrap(),
            )
            .unwrap();
            assert_eq!(
                snapshot(&mut *store.inner.writer.lock().await).await,
                expected,
                "writer state: {owner:?}, committed={committed}"
            );
            let mut reader = store.readers().acquire().await.unwrap();
            assert_eq!(
                snapshot(&mut reader).await,
                expected,
                "reader state: {owner:?}, committed={committed}"
            );
        }
    }
}
