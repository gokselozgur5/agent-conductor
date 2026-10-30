use std::path::{Path, PathBuf};

use clap::Subcommand;
use conductor_cli::send;
use conductor_protocol::{Request, RequestEnvelope, Response, ResponseResult};
use serde_json::{Value, json};

#[derive(Debug, Subcommand)]
pub enum ChatCommand {
    /// List conversations.
    Ls,
    /// Create a conversation and its room; members default to every configured agent.
    New {
        #[arg(long)]
        project: PathBuf,
        #[arg(long)]
        lead: String,
        #[arg(long = "member")]
        members: Vec<String>,
    },
    /// Post an operator message; it goes to the coordinator unless addressed.
    Send {
        conversation: String,
        #[arg(long, conflicts_with = "all")]
        to: Option<String>,
        #[arg(long)]
        all: bool,
        #[arg(required = true, num_args = 1..)]
        text: Vec<String>,
    },
}

pub async fn run(socket: &Path, command: ChatCommand) -> i32 {
    let result = match command {
        ChatCommand::Ls => list(socket).await,
        ChatCommand::New {
            project,
            lead,
            members,
        } => create(socket, &project, lead, members).await,
        ChatCommand::Send {
            conversation,
            to,
            all,
            text,
        } => post(socket, conversation, to, all, text.join(" ")).await,
    };
    result.unwrap_or_else(|message| {
        eprintln!("{message}");
        1
    })
}

async fn rpc(socket: &Path, request: Request) -> Result<Response, String> {
    let response = send(socket, &RequestEnvelope::new(request))
        .await
        .map_err(|error| error.to_string())?;
    match response.result {
        ResponseResult::Ok(response) => Ok(response),
        ResponseResult::Error(error) => Err(error.message),
    }
}

async fn list(socket: &Path) -> Result<i32, String> {
    let Response::Conversations { entries } = rpc(
        socket,
        Request::ListConversations {
            after: String::new(),
        },
    )
    .await?
    else {
        return Err("unexpected daemon response".into());
    };
    for entry in entries {
        println!(
            "{}  {}",
            entry["id"].as_str().unwrap_or("?"),
            entry["project_path"].as_str().unwrap_or("?")
        );
    }
    Ok(0)
}

/// Everyone but the lead reports to and observes the lead.
fn room_config(lead: &str, members: &[String]) -> Value {
    let mut all = vec![json!({"agent_id": lead, "reports_to": null, "observes": []})];
    for member in members.iter().filter(|m| *m != lead) {
        all.push(json!({"agent_id": member, "reports_to": lead, "observes": [lead]}));
    }
    json!({"coordinator": lead, "members": all})
}

async fn create(
    socket: &Path,
    project: &Path,
    lead: String,
    mut members: Vec<String>,
) -> Result<i32, String> {
    if !project.is_absolute() || !project.is_dir() {
        return Err(format!(
            "project folder does not exist: {}",
            project.display()
        ));
    }
    if members.is_empty() {
        let Response::ConversationAgents { agents } =
            rpc(socket, Request::ConversationAgents).await?
        else {
            return Err("unexpected daemon response".into());
        };
        members = agents
            .iter()
            .filter_map(|a| a["id"].as_str().map(String::from))
            .collect();
    }
    let id = format!("chat-{}", uuid::Uuid::now_v7());
    let project_path = project.to_string_lossy().into_owned();
    rpc(
        socket,
        Request::CreateConversation {
            id: id.clone(),
            project_path,
        },
    )
    .await?;
    let configuration = room_config(&lead, &members);
    rpc(
        socket,
        Request::ConfigureConversationRoom {
            conversation_id: id.clone(),
            configuration,
        },
    )
    .await?;
    println!("{id}");
    Ok(0)
}

fn address(message_id: String, to: Option<String>, everyone: bool) -> Request {
    Request::RouteConversationMessage {
        message_id,
        recipients: to.into_iter().collect(),
        everyone,
    }
}

async fn post(
    socket: &Path,
    conversation_id: String,
    to: Option<String>,
    everyone: bool,
    body: String,
) -> Result<i32, String> {
    let message_id = format!("msg-{}", uuid::Uuid::now_v7());
    let saved = Request::SendConversationMessage {
        conversation_id,
        message_id: message_id.clone(),
        body,
    };
    rpc(socket, saved).await?;
    rpc(socket, address(message_id, to, everyone)).await?;
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn room_reports_every_member_to_the_lead_once() {
        let room = room_config("lead", &["worker".into(), "lead".into()]);
        assert_eq!(room["coordinator"], "lead");
        assert_eq!(room["members"].as_array().map(Vec::len), Some(2));
        assert_eq!(room["members"][0]["reports_to"], Value::Null);
        assert_eq!(
            room["members"][1],
            json!({"agent_id": "worker", "reports_to": "lead", "observes": ["lead"]})
        );
    }

    #[test]
    fn unaddressed_message_goes_to_the_coordinator_only() {
        let Request::RouteConversationMessage {
            recipients,
            everyone,
            ..
        } = address("m".into(), None, false)
        else {
            panic!("wrong request")
        };
        assert!(recipients.is_empty() && !everyone);
        let Request::RouteConversationMessage { recipients, .. } =
            address("m".into(), Some("worker".into()), false)
        else {
            panic!("wrong request")
        };
        assert_eq!(recipients, ["worker"]);
    }
}
