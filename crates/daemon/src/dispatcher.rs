//! Host-owned sequential dispatch with explicitly supplied process credentials.
use conductor_service::PersistentControlService;
use std::{collections::BTreeMap, time::Duration};

pub fn authentication(kind: &str) -> BTreeMap<String, String> {
    ["HOME", "LANG", "TMPDIR", kind]
        .into_iter()
        .filter_map(|key| std::env::var(key).ok().map(|value| (key.into(), value)))
        .collect()
}
pub async fn serve(
    service: PersistentControlService,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let worker = authentication("ANTHROPIC_API_KEY");
    let verifier = authentication("CODEX_HOME");
    loop {
        if *stop.borrow() {
            return;
        }
        tokio::select! {
            biased;
            _ = stop.changed() => return,
            result = service.dispatch_next_workflow(worker.clone(), verifier.clone()) => {
                if result.is_err() {
                    eprintln!("native dispatcher stopped: dispatch ownership/storage requires reconciliation");
                    return;
                }
            }
        }
        tokio::select! {
            _ = stop.changed() => return,
            () = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
    }
}
