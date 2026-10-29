use clap::{Args as ClapArgs, Subcommand};
use conductor_runtime::{SnapshotError, SnapshotManifest, export_snapshot, import_snapshot};
use std::{
    fs::File,
    io::{self, Read},
    path::PathBuf,
};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    action: Action,
}

#[derive(Debug, Subcommand)]
enum Action {
    /// Write a binary snapshot stream to stdout; stdout must not be a terminal.
    Export {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        expected_sha256: String,
    },
    /// Read stdin into a new private snapshot; expected identity comes from its owner.
    Import {
        #[arg(long)]
        destination: PathBuf,
        #[arg(long)]
        expected_sha256: String,
    },
}

pub fn run(args: &Args) -> i32 {
    match transfer(&args.action) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("snapshot transfer refused: {error}");
            2
        }
    }
}

fn transfer(action: &Action) -> Result<(), SnapshotError> {
    match action {
        Action::Export {
            source,
            manifest,
            expected_sha256,
        } => {
            use std::io::IsTerminal;
            if io::stdout().is_terminal() {
                return Err(SnapshotError::Invalid);
            }
            let file = File::open(manifest).map_err(|_| SnapshotError::Io)?;
            let mut bytes = Vec::new();
            file.take(64 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| SnapshotError::Io)?;
            if bytes.len() > 64 * 1024 * 1024 {
                return Err(SnapshotError::Bounds);
            }
            let manifest: SnapshotManifest =
                serde_json::from_slice(&bytes).map_err(|_| SnapshotError::Invalid)?;
            export_snapshot(source, &manifest, expected_sha256, &mut io::stdout().lock())
        }
        Action::Import {
            destination,
            expected_sha256,
        } => {
            import_snapshot(&mut io::stdin().lock(), destination, expected_sha256)?;
            println!(
                "{}",
                serde_json::json!({"snapshot_sha256": expected_sha256})
            );
            Ok(())
        }
    }
}
