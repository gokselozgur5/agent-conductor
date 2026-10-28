use conductor_runtime::{CheckDefinition, CheckOutcome, SnapshotManifest, execute_check};
use std::{
    io::Read,
    path::{Path, PathBuf},
};

#[derive(Debug, clap::Args)]
pub struct CheckArgs {
    #[arg(long)]
    source: PathBuf,
    /// Previously captured `SnapshotManifest` JSON; binds the exact input bytes.
    #[arg(long)]
    manifest: PathBuf,
    /// New directory for the disposable check copy. Existing paths are refused.
    #[arg(long)]
    destination: PathBuf,
    /// Operator-approved `CheckDefinition` JSON, including exact argv and timeout.
    #[arg(long)]
    definition: PathBuf,
    /// Canonical read-only toolchain directories; repeat for multiple roots.
    #[arg(long)]
    toolchain_root: Vec<PathBuf>,
}

pub async fn run(args: &CheckArgs) -> i32 {
    match execute(args).await {
        Ok(receipt) => {
            if let Ok(json) = serde_json::to_string_pretty(&receipt) {
                println!("{json}");
            } else {
                eprintln!("failed to serialize check receipt");
                return 5;
            }
            i32::from(receipt.outcome != CheckOutcome::Passed)
        }
        Err(message) => {
            eprintln!("{message}");
            2
        }
    }
}
async fn execute(args: &CheckArgs) -> Result<conductor_runtime::CheckReceipt, String> {
    let manifest: SnapshotManifest = read_json(&args.manifest)?;
    if manifest.version != 1 || manifest.entries.len() > 10_000 {
        return Err("unsupported source manifest".into());
    }
    let definition: CheckDefinition = read_json(&args.definition)?;
    let hash = manifest.sha256().map_err(|e| e.to_string())?;
    let inventory = manifest
        .entries
        .iter()
        .map(|e| e.path.clone())
        .collect::<Vec<_>>();
    let (sender, receiver) = tokio::sync::watch::channel(false);
    let mut operation = Box::pin(execute_check(
        &args.source,
        &inventory,
        &hash,
        &args.destination,
        &definition,
        &args.toolchain_root,
        receiver,
    ));
    tokio::select! {
        result = &mut operation => result.map_err(|e| e.to_string()),
        _ = tokio::signal::ctrl_c() => {
            let _ = sender.send(true);
            operation.await.map_err(|e| e.to_string())
        }
    }
}
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    const LIMIT: u64 = 4 * 1024 * 1024;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| "cannot open check input")?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read check input")?;
    if bytes.len() as u64 > LIMIT {
        return Err("check input exceeds size limit".into());
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid check input JSON".into())
}
