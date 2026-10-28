mod dispatcher;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use agent_conductor_store::Store;
use clap::Parser;
use conductor_daemon::{LocalServer, OperatorBoard, RuntimeLock};
use conductor_service::{NativeWorkflowPlan, PersistentControlService};
use std::io::Read;
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
    /// Run one explicitly authorized native workflow while serving status, then exit.
    #[arg(long)]
    run_plan: Option<PathBuf>,
    /// Execute explicitly submitted native plans sequentially; credentials stay in process memory.
    #[arg(long, conflicts_with = "run_plan")]
    dispatch: bool,
    /// Optional authenticated loopback board; requires `CONDUCTOR_OPERATOR_TOKEN`.
    #[arg(long)]
    http: Option<std::net::SocketAddr>,
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
    let (board_stop, board_task) = start_board(args.http, service.clone()).await;
    let workflow_service = service.clone();
    let server = match LocalServer::bind(&args.socket, service) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("failed to bind {}: {error}", args.socket.display());
            std::process::exit(1);
        }
    };

    if let Some(path) = &args.run_plan {
        Box::pin(run_plan_mode(path, workflow_service, server)).await;
        stop_board(board_stop, board_task).await;
        return;
    }

    let (dispatch_stop, dispatch_rx) = tokio::sync::watch::channel(false);
    let dispatch_task = args
        .dispatch
        .then(|| tokio::spawn(dispatcher::serve(workflow_service, dispatch_rx)));
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
    let _ = dispatch_stop.send(true);
    if let Some(task) = dispatch_task {
        let _ = task.await;
    }
    stop_board(board_stop, board_task).await;
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

fn read_plan(path: &Path) -> Result<NativeWorkflowPlan, &'static str> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| "cannot open workflow plan")?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read workflow plan")?;
    if bytes.len() > 1024 * 1024 {
        return Err("workflow plan exceeds size bound");
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid workflow plan")
}

async fn run_plan_mode(
    path: &Path,
    workflow_service: PersistentControlService,
    server: LocalServer<PersistentControlService>,
) {
    let plan = match read_plan(path) {
        Ok(plan) => plan,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    let worker_auth = dispatcher::authentication("ANTHROPIC_API_KEY");
    let verifier_auth = if serde_json::to_value(&plan.verifier)
        .ok()
        .and_then(|v| v.get("kind").cloned())
        == Some(serde_json::json!("claude"))
    {
        dispatcher::authentication("ANTHROPIC_API_KEY")
    } else {
        dispatcher::authentication("CODEX_HOME")
    };
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let serving = server.run_until(async move {
        let _ = stopped.changed().await;
    });
    let operation = async {
        tokio::select! {
            result = workflow_service.run_native_workflow(&plan, worker_auth, verifier_auth) => result.map_err(|e| e.to_string()),
            _ = tokio::signal::ctrl_c() => Err("workflow interrupted; durable task and artifacts preserved".into()),
        }
    };
    let run = async {
        let result = operation.await;
        let _ = stop.send(true);
        result
    };
    let (transport_result, result) = tokio::join!(serving, run);
    if let Err(error) = transport_result {
        eprintln!("status server failed: {error}");
    }
    match result {
        Ok(outcome) => {
            if let Ok(json) = serde_json::to_string_pretty(&outcome) {
                println!("{json}");
            } else {
                eprintln!("workflow result encoding failed");
                std::process::exit(5);
            }
            if !outcome.completed {
                std::process::exit(1);
            }
        }
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
    }
}

type BoardTask = tokio::task::JoinHandle<std::io::Result<()>>;
async fn start_board(
    address: Option<std::net::SocketAddr>,
    service: PersistentControlService,
) -> (tokio::sync::watch::Sender<bool>, Option<BoardTask>) {
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let Some(address) = address else {
        return (stop, None);
    };
    let token = std::env::var("CONDUCTOR_OPERATOR_TOKEN").unwrap_or_else(|_| {
        eprintln!("--http requires CONDUCTOR_OPERATOR_TOKEN in the launching environment");
        std::process::exit(2);
    });
    let board = OperatorBoard::bind(address, token, service)
        .await
        .unwrap_or_else(|error| {
            eprintln!("operator board failed: {error}");
            std::process::exit(2);
        });
    eprintln!("Operator board: http://{}", board.address());
    let task = tokio::spawn(board.run_until(async move {
        let _ = stopped.changed().await;
    }));
    (stop, Some(task))
}
async fn stop_board(stop: tokio::sync::watch::Sender<bool>, task: Option<BoardTask>) {
    let _ = stop.send(true);
    if let Some(mut task) = task {
        match tokio::time::timeout(std::time::Duration::from_secs(5), &mut task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(_) => eprintln!("operator board did not shut down cleanly"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                eprintln!("operator board shutdown deadline elapsed");
            }
        }
    }
}
