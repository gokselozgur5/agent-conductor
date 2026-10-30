use std::path::Path;

use clap::Subcommand;
use conductor_cli::send;
use conductor_protocol::{Request, RequestEnvelope, Response, ResponseResult};
use serde_json::{Value, json};

#[derive(Debug, Subcommand)]
pub enum ChatCommand {
    /// List conversations.
    Ls,
}

pub async fn run(socket: &Path, command: ChatCommand) -> i32 {
    let result = match command {
        ChatCommand::Ls => list(socket).await,
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

#[allow(dead_code)] // Used by `chat new` in the next commit.
/// Everyone but the lead reports to and observes the lead.
fn room_config(lead: &str, members: &[String]) -> Value {
    let mut all = vec![json!({"agent_id": lead, "reports_to": null, "observes": []})];
    for member in members.iter().filter(|m| *m != lead) {
        all.push(json!({"agent_id": member, "reports_to": lead, "observes": [lead]}));
    }
    json!({"coordinator": lead, "members": all})
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
}
