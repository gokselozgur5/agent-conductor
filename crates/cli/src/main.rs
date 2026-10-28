use std::path::PathBuf;

use clap::{Parser, Subcommand};
use conductor_cli::{response_exit_code, send};
use conductor_protocol::{
    ConsumerStateInput, EventSubscriptionInput, Request, RequestEnvelope, Response, ResponseResult,
    SemanticEventInput,
};

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
        path: String,
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
    /// Bind an operator-supplied Phase 1 source label; does not capture or fence Git.
    BindVerificationSource {
        task_id: String,
        expected_version: u64,
        idempotency_key: String,
        source: String,
    },
    SubscribeEvents {
        subscription_json: String,
    },
    PublishEvent {
        event_json: String,
    },
    ClaimEvent {
        agent_id: String,
    },
    AckEvent {
        delivery_id: String,
        consumer_agent_id: String,
        claim_token: String,
        effect_json: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let request = RequestEnvelope::new(command_request(cli.command));

    let response = match send(&cli.socket, &request).await {
        Ok(response) => response,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(4);
        }
    };

    let status = response_exit_code(&response);
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
    if status != 0 {
        std::process::exit(status);
    }
}

fn command_request(command: Command) -> Request {
    match command {
        Command::Health => Request::Health,
        Command::CheckInvariants => Request::CheckInvariants,
        Command::RegisterProject {
            actor_id,
            idempotency_key,
            id,
            path,
        } => Request::RegisterProject {
            actor_id,
            idempotency_key,
            id,
            path,
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
        Command::BindVerificationSource {
            task_id,
            expected_version,
            idempotency_key,
            source,
        } => Request::BindVerificationSource {
            task_id,
            expected_version,
            idempotency_key,
            source: agent_conductor_domain::SourceIdentity(source),
        },
        Command::SubscribeEvents { subscription_json } => Request::SubscribeEvents {
            subscription: parse_json::<EventSubscriptionInput>(
                &subscription_json,
                "event subscription",
            ),
        },
        Command::PublishEvent { event_json } => Request::PublishEvent {
            event: parse_json::<SemanticEventInput>(&event_json, "semantic event"),
        },
        Command::ClaimEvent { agent_id } => Request::ClaimEvent { agent_id },
        Command::AckEvent {
            delivery_id,
            consumer_agent_id,
            claim_token,
            effect_json,
        } => Request::AcknowledgeEvent {
            delivery_id,
            consumer_agent_id,
            claim_token,
            effect: parse_json::<ConsumerStateInput>(&effect_json, "consumer state effect"),
        },
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
        }
    }
}

fn parse_json<T: serde::de::DeserializeOwned>(input: &str, label: &str) -> T {
    serde_json::from_str(input).unwrap_or_else(|error| {
        eprintln!("invalid {label} JSON: {error}");
        std::process::exit(2)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_binding_cli_maps_to_privileged_request() {
        let cli = Cli::try_parse_from([
            "conductorctl",
            "bind-verification-source",
            "task",
            "4",
            "bind-1",
            "fake:phase-1:artifact",
        ])
        .expect("source binding command parses");
        let Request::BindVerificationSource {
            task_id,
            expected_version,
            idempotency_key,
            source,
        } = command_request(cli.command)
        else {
            panic!("wrong request")
        };
        assert_eq!(task_id, "task");
        assert_eq!(expected_version, 4);
        assert_eq!(idempotency_key, "bind-1");
        assert_eq!(source.0, "fake:phase-1:artifact");
    }
}
