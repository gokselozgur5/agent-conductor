use std::path::PathBuf;

use clap::{Parser, Subcommand};
use conductor_cli::send;
use conductor_protocol::{Request, RequestEnvelope, Response, ResponseResult};

#[derive(Debug, Parser)]
#[command(name = "conductorctl", version, about = "Agent Conductor local client")]
struct Cli {
    #[arg(long, default_value = "state/conductor.sock")]
    socket: PathBuf,
    #[arg(long)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Health,
    CheckInvariants,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let payload = match cli.command {
        Command::Health => Request::Health,
        Command::CheckInvariants => Request::CheckInvariants,
    };
    let request = RequestEnvelope::new(payload);

    let response = match send(&cli.socket, &request).await {
        Ok(response) => response,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(4);
        }
    };

    if cli.json {
        match serde_json::to_string_pretty(&response) {
            Ok(json) => println!("{json}"),
            Err(error) => {
                eprintln!("failed to encode response: {error}");
                std::process::exit(5);
            }
        }
    } else {
        print_human(response.result);
    }
}

fn print_human(result: ResponseResult) {
    match result {
        ResponseResult::Ok(Response::Health(health)) => println!(
            "status={:?} boot_generation={} uptime_ms={}",
            health.status, health.boot_generation, health.uptime_millis
        ),
        ResponseResult::Ok(Response::Invariants(invariants)) => {
            println!(
                "healthy={} violations={}",
                invariants.healthy,
                invariants.violations.len()
            );
            for violation in invariants.violations {
                println!("{}: {}", violation.code, violation.summary);
            }
        }
        ResponseResult::Error(error) => {
            eprintln!("{:?}: {}", error.code, error.message);
            std::process::exit(2);
        }
    }
}
