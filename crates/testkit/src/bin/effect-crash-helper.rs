use std::{env, process};

use agent_conductor_store::Store;
use agent_conductor_testkit::{CrashPoint, FakeEffectRunner, FakeInventory};
use sqlx::{Connection, Executor, SqliteConnection};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("effect crash helper failed: {error}");
        process::exit(2);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("before-intent-commit") => {
            let database = required(&args, 2)?;
            let token = required(&args, 3)?;
            let mut connection = SqliteConnection::connect(&format!("sqlite://{database}")).await?;
            connection.execute("BEGIN IMMEDIATE").await?;
            sqlx::query(
                "INSERT INTO effect_intents (token, correlation_id, effect_kind, request_json, status, created_at_ms, updated_at_ms) VALUES (?, 'corr', 'worker.spawn', '{}', 'pending', 100, 100)",
            )
            .bind(token)
            .execute(&mut connection)
            .await?;
            process::abort();
        }
        Some("reconcile") => {
            let database = required(&args, 2)?;
            let inventory_path = required(&args, 3)?;
            let token = required(&args, 4)?;
            let now_ms = required(&args, 5)?.parse::<i64>()?;
            let crash = parse_crash(required(&args, 6)?)?;
            let store = Store::open(database).await?;
            let inventory = FakeInventory::open(inventory_path).await?;
            let runner = FakeEffectRunner::new(store, inventory, "fake-runner", 50);
            let _ = runner.reconcile(token, now_ms, crash).await?;
            Ok(())
        }
        _ => Err("unknown helper command".into()),
    }
}

fn required(args: &[String], index: usize) -> Result<&str, Box<dyn std::error::Error>> {
    args.get(index)
        .map(String::as_str)
        .ok_or_else(|| format!("missing argument {index}").into())
}

fn parse_crash(value: &str) -> Result<CrashPoint, Box<dyn std::error::Error>> {
    match value {
        "before_claim" => Ok(CrashPoint::BeforeClaim),
        "after_claim" => Ok(CrashPoint::AfterClaim),
        "after_external_effect" => Ok(CrashPoint::AfterExternalEffect),
        "after_observation" => Ok(CrashPoint::AfterObservation),
        "after_done" => Ok(CrashPoint::AfterDone),
        _ => Err(format!("unknown crash point {value}").into()),
    }
}
