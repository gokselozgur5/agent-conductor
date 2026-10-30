//! Durable conversation queue supervision. Never drop a running native future
//! on shutdown: signal cancellation and await its persisted terminal observation.
use conductor_service::{ConversationAgent, PersistentControlService};
use std::{io::Read, path::Path, time::Duration};

pub fn configure(
    service: &PersistentControlService,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(262_145)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 262_144 {
        return Err("conversation configuration too large".into());
    }
    let agents: Vec<ConversationAgent> = serde_json::from_slice(&bytes)?;
    service.configure_conversation_agents(agents)?;
    Ok(())
}

pub async fn serve(
    service: PersistentControlService,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    while !*stop.borrow() {
        match service.dispatch_next_conversation(stop.clone()).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(_) => {
                eprintln!("conversation dispatcher stopped: persistence requires reconciliation");
                return;
            }
        }
        tokio::select! {
            _ = stop.changed() => return,
            () = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
}
