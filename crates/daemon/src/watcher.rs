//! Opt-in bounded repository observation; no worker launch authority.
use conductor_service::{PersistentControlService, RepositoryWatch, RepositoryWatchState};
use std::{
    collections::BTreeSet,
    io::Read,
    path::Path,
    time::{Duration, Instant},
};

pub fn load(path: &Path) -> Result<Vec<RepositoryWatch>, &'static str> {
    let file = std::fs::File::open(path).map_err(|_| "watch configuration cannot be read")?;
    let mut bytes = Vec::new();
    file.take(65_537)
        .read_to_end(&mut bytes)
        .map_err(|_| "watch configuration read failed")?;
    if bytes.len() > 65_536 {
        return Err("watch configuration too large");
    }
    let configs: Vec<RepositoryWatch> =
        serde_json::from_slice(&bytes).map_err(|_| "invalid watch configuration")?;
    let mut ids = BTreeSet::new();
    if configs.is_empty() || configs.len() > 16 || configs.iter().any(|c| !ids.insert(&c.id)) {
        return Err("watch count or identities invalid");
    }
    Ok(configs)
}

pub async fn serve(
    service: PersistentControlService,
    configs: Vec<RepositoryWatch>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let started = Instant::now();
    let mut states: Vec<_> = configs
        .iter()
        .map(|_| RepositoryWatchState::default())
        .collect();
    loop {
        for (config, state) in configs.iter().zip(&mut states) {
            if *stop.borrow() {
                return;
            }
            // Let the atomic publication finish before graceful shutdown.
            // Dropping a manually managed `SQLite` transaction mid-await would
            // leave its writer connection in an indeterminate transaction.
            if service
                .poll_repository(config, state, started.elapsed())
                .await
                .is_err()
            {
                eprintln!(
                    "repository watcher stopped: observation or publication refused; inspect configuration and retained baseline"
                );
                return;
            }
        }
        tokio::select! {
            _ = stop.changed() => return,
            () = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
}
