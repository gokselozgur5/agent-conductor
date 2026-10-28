//! Opt-in bounded repository observation; no worker launch authority.
use conductor_protocol::WatcherStatus;
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
    let mut health = WatchHealth {
        service: service.clone(),
        graceful: false,
    };
    let started = Instant::now();
    let mut owned_states = std::collections::BTreeMap::new();
    let mut states: Vec<_> = configs
        .iter()
        .map(|_| RepositoryWatchState::default())
        .collect();
    loop {
        if *stop.borrow() {
            health.graceful = true;
            return;
        }
        for (config, state) in configs.iter().zip(&mut states) {
            if *stop.borrow() {
                health.graceful = true;
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
        for project in configs
            .iter()
            .map(|config| &config.project_id)
            .collect::<BTreeSet<_>>()
        {
            if service
                .poll_owned_workspaces(
                    project,
                    owned_states.entry(project.clone()).or_default(),
                    started.elapsed(),
                )
                .await
                .is_err()
            {
                eprintln!("repository watcher stopped: owned observation refused");
                return;
            }
        }
        service.set_watcher_status(WatcherStatus::Running);
        tokio::select! {
            _ = stop.changed() => { health.graceful = true; return; },
            () = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
}

// Dropping an aborted or panicking watcher must not leave health reporting Running.
struct WatchHealth {
    service: PersistentControlService,
    graceful: bool,
}
impl Drop for WatchHealth {
    fn drop(&mut self) {
        self.service.set_watcher_status(if self.graceful {
            WatcherStatus::Stopped
        } else {
            WatcherStatus::Failed
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use conductor_protocol::{HealthStatus, Request, Response};
    use conductor_service::ControlService;

    async fn assert_health(
        service: &PersistentControlService,
        watcher: WatcherStatus,
        status: HealthStatus,
    ) {
        let Response::Health(health) = service.handle(Request::Health).await.unwrap() else {
            panic!("health response")
        };
        assert_eq!(health.repository_watcher, watcher);
        assert_eq!(health.status, status);
    }

    #[tokio::test]
    async fn watcher_failure_and_abort_are_visible_through_shared_service_health() {
        let directory = tempfile::tempdir().unwrap();
        let store = agent_conductor_store::Store::open(directory.path().join("state.sqlite3"))
            .await
            .unwrap();
        let service = PersistentControlService::start(store, 1).await.unwrap();
        assert_health(&service, WatcherStatus::Disabled, HealthStatus::Ready).await;
        service.set_watcher_status(WatcherStatus::Starting);
        assert_health(
            &service.clone(),
            WatcherStatus::Starting,
            HealthStatus::Degraded,
        )
        .await;
        let (_stop, receiver) = tokio::sync::watch::channel(false);
        serve(
            service.clone(),
            vec![RepositoryWatch {
                id: "invalid".into(),
                project_id: "missing".into(),
                source_agent_id: "missing".into(),
                root: directory.path().join("missing"),
            }],
            receiver,
        )
        .await;
        assert_health(&service, WatcherStatus::Failed, HealthStatus::Degraded).await;
        let (stop, receiver) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(serve(service.clone(), vec![], receiver));
        for _ in 0..100 {
            let Response::Health(h) = service.handle(Request::Health).await.unwrap() else {
                panic!("health")
            };
            if h.repository_watcher == WatcherStatus::Running {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_health(&service, WatcherStatus::Running, HealthStatus::Ready).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_health(&service, WatcherStatus::Failed, HealthStatus::Degraded).await;
        let task = tokio::spawn(serve(service.clone(), vec![], stop.subscribe()));
        stop.send(true).unwrap();
        task.await.unwrap();
        assert_health(&service, WatcherStatus::Stopped, HealthStatus::Degraded).await;
    }
}
