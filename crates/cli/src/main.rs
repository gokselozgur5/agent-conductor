use std::path::PathBuf;

use clap::{Parser, Subcommand};
use conductor_cli::{exit_code, send};
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
    RegisterProject {
        actor_id: String,
        idempotency_key: String,
        id: String,
        canonical_path: String,
        filesystem_identity: String,
        #[arg(long)]
        git_common_dir_identity: Option<String>,
    },
    RegisterAgent {
        actor_id: String,
        idempotency_key: String,
        id: String,
        adapter_kind: String,
        protocol_fingerprint: String,
        capability_hash: String,
        access_mode: String,
    },
    CreateTask {
        actor_id: String,
        idempotency_key: String,
        id: String,
        project_id: String,
        objective: String,
        #[arg(long, default_value_t = 0)]
        priority: i64,
        #[arg(long, default_value = "{}")]
        authority_json: String,
        #[arg(long, default_value = "[]")]
        required_checks_json: String,
    },
    GetTask {
        task_id: String,
    },
    ListTasks {
        #[arg(long)]
        project_id: Option<String>,
    },
    Apply {
        task_id: String,
        command_json: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let payload = match cli.command {
        Command::Health => Request::Health,
        Command::CheckInvariants => Request::CheckInvariants,
        Command::RegisterProject {
            actor_id,
            idempotency_key,
            id,
            canonical_path,
            filesystem_identity,
            git_common_dir_identity,
        } => Request::RegisterProject {
            actor_id,
            idempotency_key,
            id,
            canonical_path,
            filesystem_identity,
            git_common_dir_identity,
        },
        Command::RegisterAgent {
            actor_id,
            idempotency_key,
            id,
            adapter_kind,
            protocol_fingerprint,
            capability_hash,
            access_mode,
        } => Request::RegisterAgent {
            actor_id,
            idempotency_key,
            id,
            adapter_kind,
            protocol_fingerprint,
            capability_hash,
            access_mode,
        },
        Command::CreateTask {
            actor_id,
            idempotency_key,
            id,
            project_id,
            objective,
            priority,
            authority_json,
            required_checks_json,
        } => Request::CreateTask {
            actor_id,
            idempotency_key,
            id,
            project_id,
            objective,
            priority,
            authority: parse_json(&authority_json, "authority"),
            required_checks: parse_json(&required_checks_json, "required checks"),
        },
        Command::GetTask { task_id } => Request::GetTask { task_id },
        Command::ListTasks { project_id } => Request::ListTasks { project_id },
        Command::Apply {
            task_id,
            command_json,
        } => Request::ApplyCommand {
            task_id,
            command: parse_json(&command_json, "command"),
        },
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
        ResponseResult::Ok(response) => println!("{response:?}"),
        ResponseResult::Error(error) => {
            eprintln!("{:?}: {}", error.code, error.message);
            std::process::exit(exit_code(error.code));
        }
    }
}

fn parse_json<T: serde::de::DeserializeOwned>(input: &str, label: &str) -> T {
    serde_json::from_str(input).unwrap_or_else(|error| {
        eprintln!("invalid {label} JSON: {error}");
        std::process::exit(2)
    })
}
