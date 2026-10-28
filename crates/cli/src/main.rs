mod check_command;
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
    /// Run an approved check in a disposable source copy (does not complete a task).
    RunCheck(check_command::CheckArgs),
    /// Read-only bounded advisory references from the current Git inventory.
    InspectProject {
        project: PathBuf,
    },
    Health,
    CheckInvariants,
    /// Persist an approved native plan for an explicitly enabled dispatcher.
    SubmitWorkflow {
        plan: PathBuf,
    },
    ListWorkflows,
    /// Continue an unfinished task only after prior write fencing is verified.
    ContinueWorkflow {
        task_id: String,
        expected_version: u64,
    },
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
    TaskCheckpoint {
        task_id: String,
    },
    GetTask {
        task_id: String,
    },
    /// Read at most 100 tasks in task-ID order; use `next_after` to advance.
    ListTaskPage {
        #[arg(long)]
        project_id: Option<String>,
        #[arg(long)]
        after: Option<String>,
    },
    TaskHistory {
        task_id: String,
        #[arg(long)]
        before: Option<String>,
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
    if let Command::RunCheck(args) = &cli.command {
        let status = check_command::run(args).await;
        std::process::exit(status);
    }
    if let Command::InspectProject { project } = &cli.command {
        if !project.is_absolute() {
            eprintln!("project path must be absolute");
            std::process::exit(2);
        }
        let result = match conductor_runtime::observe_git(project).await {
            Ok(snapshot) => conductor_runtime::discover_advisory(&snapshot),
            Err(error) => Err(error),
        };
        let Ok(inventory) = result else {
            eprintln!("project observation refused; inspect path, Git layout and file bounds");
            std::process::exit(2);
        };
        let Ok(json) = serde_json::to_string_pretty(&inventory) else {
            std::process::exit(5);
        };
        println!("{json}");
        return;
    }
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

#[allow(clippy::too_many_lines)] // Keep the declarative CLI-to-protocol mapping together.
fn command_request(command: Command) -> Request {
    match command {
        Command::RunCheck(_) | Command::InspectProject { .. } => {
            eprintln!("local inspection/check execution is not a daemon request");
            std::process::exit(2);
        }
        Command::Health => Request::Health,
        Command::CheckInvariants => Request::CheckInvariants,
        Command::ListWorkflows => Request::ListWorkflows,
        Command::ContinueWorkflow {
            task_id,
            expected_version,
        } => Request::ContinueWorkflow {
            task_id,
            expected_version,
        },
        Command::SubmitWorkflow { plan } => read_workflow_plan(plan),
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
        Command::TaskCheckpoint { task_id } => Request::TaskCheckpoint { task_id },
        Command::GetTask { task_id } => Request::GetTask { task_id },
        Command::ListTaskPage { project_id, after } => Request::ListTaskPage { project_id, after },
        Command::TaskHistory { task_id, before } => Request::TaskHistory { task_id, before },
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

fn read_workflow_plan(plan: PathBuf) -> Request {
    use std::io::Read;
    let mut bytes = Vec::new();
    let result =
        std::fs::File::open(plan).and_then(|file| file.take(131_073).read_to_end(&mut bytes));
    if result.is_err() || bytes.len() > 131_072 {
        eprintln!("cannot read native plan within 128 KiB bound");
        std::process::exit(2);
    }
    let Ok(plan_json) = String::from_utf8(bytes) else {
        eprintln!("plan must be UTF-8");
        std::process::exit(2);
    };
    Request::SubmitWorkflow { plan_json }
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
