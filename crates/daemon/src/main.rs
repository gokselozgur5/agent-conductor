use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use agent_conductor_store::Store;
use clap::Parser;
use conductor_daemon::{LocalServer, RuntimeLock};
use conductor_service::PersistentControlService;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "conductor-daemon",
    version,
    about = "Agent Conductor local daemon"
)]
struct Args {
    #[arg(long, default_value = "state/conductor.sqlite3")]
    database: PathBuf,
    #[arg(long, default_value = "state/conductor.sock")]
    socket: PathBuf,
    #[arg(long, default_value = "state/conductor.lock")]
    lock: PathBuf,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    if let Err(error) = ensure_parent(&args.database)
        .and_then(|()| ensure_parent(&args.socket))
        .and_then(|()| ensure_parent(&args.lock))
    {
        eprintln!("failed to prepare runtime paths: {error}");
        std::process::exit(1);
    }
    let _runtime_lock = match RuntimeLock::acquire(&args.lock, &args.socket) {
        Ok(lock) => lock,
        Err(error) => {
            eprintln!("failed to acquire daemon ownership: {error}");
            std::process::exit(1);
        }
    };
    let started_at_ms = match unix_time_ms() {
        Ok(value) => value,
        Err(error) => {
            eprintln!("failed to read wall clock: {error}");
            std::process::exit(1);
        }
    };
    let store = match Store::open(&args.database).await {
        Ok(store) => store,
        Err(error) => {
            eprintln!("failed to open {}: {error}", args.database.display());
            std::process::exit(1);
        }
    };
    let service = match PersistentControlService::start(store, started_at_ms).await {
        Ok(service) => service,
        Err(error) => {
            eprintln!("runtime startup checks failed: {error}");
            std::process::exit(1);
        }
    };
    let server = match LocalServer::bind(&args.socket, service) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("failed to bind {}: {error}", args.socket.display());
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

fn ensure_parent(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn unix_time_ms() -> Result<i64, std::time::SystemTimeError> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(i64::try_from(millis).unwrap_or(i64::MAX))
}
