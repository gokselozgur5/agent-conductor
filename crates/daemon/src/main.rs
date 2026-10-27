use std::path::PathBuf;

use conductor_daemon::LocalServer;
use conductor_service::InMemoryControlService;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let socket = std::env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("state/conductor.sock"), PathBuf::from);
    let service = InMemoryControlService::new(1);
    let server = match LocalServer::bind(&socket, service) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("failed to bind {}: {error}", socket.display());
            std::process::exit(1);
        }
    };

    if let Err(error) = server
        .run_until(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(%error, "failed to install shutdown signal");
            }
        })
        .await
    {
        eprintln!("daemon failed: {error}");
        std::process::exit(1);
    }
}
