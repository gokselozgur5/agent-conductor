use std::path::Path;

use clap::Subcommand;
use conductor_cli::send;
use conductor_protocol::{Request, RequestEnvelope, Response, ResponseResult};

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
