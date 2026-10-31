use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use clap::Subcommand;
use conductor_cli::send;
use conductor_protocol::{Request, RequestEnvelope, Response, ResponseResult};
use serde_json::{Value, json};

const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

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
    /// Show each request as a tree of assignments, reports, blockers and summary.
    Tree { conversation: String },
    /// Show the room history; --follow keeps printing new messages and state changes.
    Log {
        conversation: String,
        #[arg(long)]
        follow: bool,
    },
    /// Chat in the room: live history, `@agent text`, `@all text`, `/tree`, `/quit`.
    Open { conversation: String },
}

pub async fn run(socket: &Path, json: bool, command: ChatCommand) -> i32 {
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
        ChatCommand::Tree { conversation } => show_tree(socket, &conversation).await.map(|()| 0),
        ChatCommand::Log {
            conversation,
            follow,
        } => history(socket, &conversation, json, follow).await,
        ChatCommand::Open { conversation } => open(socket, conversation).await,
    };
    result.unwrap_or_else(|message| {
        eprintln!("{message}");
        1
    })
}

async fn rpc(socket: &Path, request: Request) -> Result<Response, String> {
    let envelope = RequestEnvelope::new(request);
    let response = tokio::time::timeout(RPC_TIMEOUT, send(socket, &envelope))
        .await
        .map_err(|_| format!("daemon did not answer within 10 s: {}", socket.display()))?
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

/// Reads every page after `after`, returning the messages and the last sequence.
async fn page(
    socket: &Path,
    conversation_id: &str,
    mut after: i64,
) -> Result<(Vec<Value>, i64), String> {
    let mut all = Vec::new();
    loop {
        let request = Request::ConversationMessages {
            conversation_id: conversation_id.into(),
            after,
        };
        let Response::ConversationMessages {
            messages,
            next_after,
        } = rpc(socket, request).await?
        else {
            return Err("unexpected daemon response".into());
        };
        after = messages
            .last()
            .and_then(|m| m["sequence"].as_i64())
            .unwrap_or(after);
        all.extend(messages);
        if next_after.is_none() {
            return Ok((all, after));
        }
    }
}

/// Recipients that were given work, with their per-recipient state.
fn recipients(message: &Value) -> String {
    let routes = message["routing"].as_array().map_or(&[][..], Vec::as_slice);
    let given: Vec<String> = routes
        .iter()
        .filter(|r| r["kind"] != "information")
        .map(|r| {
            format!(
                "{} ({})",
                r["agent_id"].as_str().unwrap_or("?"),
                r["state"].as_str().unwrap_or("?")
            )
        })
        .collect();
    if given.is_empty() {
        String::new()
    } else {
        format!(" → {}", given.join(", "))
    }
}

/// An authorized decision reads as its assignments; anything else as its text.
fn body(message: &Value) -> String {
    let text = message["body"].as_str().unwrap_or_default();
    if message["examination"]["outcome"] == "authorized"
        && let Ok(plan) = serde_json::from_str::<Value>(text)
        && let Some(assignments) = plan["assignments"].as_array()
    {
        return assignments
            .iter()
            .map(|a| {
                format!(
                    "[task for {}] {}",
                    a["agent_id"].as_str().unwrap_or("?"),
                    a["instruction"].as_str().unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
    }
    text.to_owned()
}

fn render(message: &Value) -> String {
    let author = message["agent_id"].as_str().unwrap_or("you");
    let mut out = format!("#{} {author}{}\n", message["sequence"], recipients(message));
    for line in body(message).lines() {
        out.push_str("    ");
        out.push_str(line);
        out.push('\n');
    }
    if message["examination"]["outcome"] == "rejected" {
        let _ = writeln!(
            out,
            "    ! delegation refused: {}",
            message["examination"]["detail"].as_str().unwrap_or("?")
        );
    }
    out
}

fn summary_line(message: &Value) -> String {
    let text = body(message);
    let first = text
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or_default();
    let mut line: String = first.chars().take(90).collect();
    if first.chars().count() > 90 || text.lines().filter(|l| !l.trim().is_empty()).count() > 1 {
        line.push('…');
    }
    line
}

fn outstanding(state: &str) -> &str {
    match state {
        "blocked" => "! blocked",
        "unknown" => "! outcome unknown, not retried",
        "failed" => "! failed",
        "launch_intent" => "working",
        other => other,
    }
}

fn branch(out: &mut String, all: &[Value], message: &Value, depth: usize) {
    let id = &message["id"];
    let parent_author = all
        .iter()
        .find(|m| m["id"] == message["parent"])
        .map(|m| &m["agent_id"]);
    let role = match message["agent_id"].as_str() {
        None => "you".to_owned(),
        Some(author) if parent_author == Some(&message["agent_id"]) => {
            format!("{author} (summary)")
        }
        Some(author) => author.to_owned(),
    };
    let _ = writeln!(
        out,
        "{}{role}: {}",
        "   ".repeat(depth),
        summary_line(message)
    );
    let children: Vec<&Value> = all.iter().filter(|m| m["parent"] == *id).collect();
    for route in message["routing"].as_array().into_iter().flatten() {
        let answered = children.iter().any(|c| c["agent_id"] == route["agent_id"]);
        if route["kind"] != "information" && !answered {
            let agent = route["agent_id"].as_str().unwrap_or("?");
            let state = outstanding(route["state"].as_str().unwrap_or("?"));
            let _ = writeln!(out, "{}{agent}: {state}", "   ".repeat(depth + 1));
        }
    }
    for child in children {
        branch(out, all, child, depth + 1);
    }
}

/// Operator requests are roots; replies hang under the message they answer.
fn tree(messages: &[Value]) -> String {
    let mut out = String::new();
    for root in messages.iter().filter(|m| m["agent_id"].is_null()) {
        branch(&mut out, messages, root, 0);
        out.push('\n');
    }
    out
}

/// Prints what is new since the last call: unseen messages and changed recipient states.
fn show_new(
    shown: &mut std::collections::BTreeMap<String, String>,
    messages: Vec<Value>,
    json: bool,
) {
    for message in messages {
        let state = recipients(&message);
        match shown.insert(message["sequence"].to_string(), state.clone()) {
            None if json => println!("{message}"),
            None => println!("{}", render(&message)),
            Some(before) if before != state && !json => {
                println!(
                    "#{} {}{state}\n",
                    message["sequence"],
                    message["agent_id"].as_str().unwrap_or("you")
                );
            }
            Some(_) => {}
        }
    }
}

async fn show_tree(socket: &Path, conversation_id: &str) -> Result<(), String> {
    let (messages, _) = page(socket, conversation_id, 0).await?;
    print!("{}", tree(&messages));
    Ok(())
}

async fn history(
    socket: &Path,
    conversation_id: &str,
    json: bool,
    follow: bool,
) -> Result<i32, String> {
    let mut shown = std::collections::BTreeMap::new();
    loop {
        let (messages, _) = page(socket, conversation_id, 0).await?;
        show_new(&mut shown, messages, json);
        if !follow {
            return Ok(0);
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// One line typed into the chat window.
#[derive(Debug, PartialEq, Eq)]
enum Input {
    Say {
        to: Option<String>,
        everyone: bool,
        text: String,
    },
    Tree,
    Quit,
    Skip,
    Invalid(String),
}

fn parse_input(line: &str) -> Input {
    let line = line.trim();
    match line {
        "" => return Input::Skip,
        "/tree" => return Input::Tree,
        "/quit" => return Input::Quit,
        _ if line.starts_with('/') => return Input::Invalid(format!("unknown command: {line}")),
        _ => {}
    }
    let Some(addressed) = line.strip_prefix('@') else {
        return Input::Say {
            to: None,
            everyone: false,
            text: line.to_owned(),
        };
    };
    let (target, text) = addressed
        .split_once(char::is_whitespace)
        .map_or((addressed, ""), |(target, text)| (target, text.trim()));
    if target.is_empty() || text.is_empty() {
        return Input::Invalid(format!("nothing to send: use @{target} <text>"));
    }
    let everyone = target == "all";
    Input::Say {
        to: (!everyone).then(|| target.to_owned()),
        everyone,
        text: text.to_owned(),
    }
}

/// Stdin lines arrive on a channel; it closes at EOF.
fn stdin_lines() -> tokio::sync::mpsc::Receiver<String> {
    let (sender, receiver) = tokio::sync::mpsc::channel(16);
    std::thread::spawn(move || {
        for line in std::io::stdin().lines().map_while(Result::ok) {
            if sender.blocking_send(line).is_err() {
                break;
            }
        }
    });
    receiver
}

async fn open(socket: &Path, conversation: String) -> Result<i32, String> {
    let mut lines = stdin_lines();
    let mut shown = std::collections::BTreeMap::new();
    let mut poll = tokio::time::interval(std::time::Duration::from_secs(2));
    loop {
        tokio::select! {
            _ = poll.tick() => {
                let (messages, _) = page(socket, &conversation, 0).await?;
                show_new(&mut shown, messages, false);
            }
            line = lines.recv() => match parse_input(&line.unwrap_or_else(|| "/quit".into())) {
                Input::Quit => return Ok(0),
                Input::Skip => {}
                Input::Invalid(message) => eprintln!("{message}"),
                Input::Tree => {
                    if let Err(message) = show_tree(socket, &conversation).await {
                        eprintln!("{message}");
                    }
                }
                Input::Say { to, everyone, text } => {
                    if let Err(message) = post(socket, conversation.clone(), to, everyone, text).await {
                        eprintln!("{message}");
                    }
                }
            },
        }
    }
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

    fn message(sender: Option<&str>, body: &str, outcome: Option<&str>, routing: &Value) -> Value {
        let examination = outcome.map(|o| json!({"outcome": o, "detail": "agent not in room"}));
        json!({"sequence": 2, "agent_id": sender, "body": body, "examination": examination, "routing": routing})
    }

    #[test]
    fn authorized_decision_reads_as_tasks_with_recipient_state() {
        let decision = r#"{"schema":"conductor.delegation.v1","assignments":[{"agent_id":"worker","instruction":"write fib.py"}]}"#;
        let routing = json!([{"agent_id": "worker", "kind": "delegated", "state": "unknown"},
            {"agent_id": "worker", "kind": "information", "state": "pending"}]);
        let shown = render(&message(
            Some("lead"),
            decision,
            Some("authorized"),
            &routing,
        ));
        assert_eq!(
            shown,
            "#2 lead → worker (unknown)\n    [task for worker] write fib.py\n"
        );
    }

    #[test]
    fn refused_decision_keeps_its_text_and_reason() {
        let shown = render(&message(
            Some("lead"),
            "{\"x\":1}",
            Some("rejected"),
            &json!([]),
        ));
        assert_eq!(
            shown,
            "#2 lead\n    {\"x\":1}\n    ! delegation refused: agent not in room\n"
        );
    }

    #[test]
    fn tree_shows_tasks_reports_blockers_and_summary() {
        let plan = r#"{"assignments":[{"agent_id":"worker","instruction":"write fib.py"},{"agent_id":"tester","instruction":"test it"}]}"#;
        let messages = [
            json!({"id": "op", "agent_id": null, "parent": null, "body": "fib please",
                "routing": [{"agent_id": "lead", "kind": "assignment", "state": "replied"}]}),
            json!({"id": "d", "agent_id": "lead", "parent": "op", "body": plan, "examination": {"outcome": "authorized"},
                "routing": [{"agent_id": "worker", "kind": "delegated", "state": "replied"},
                    {"agent_id": "tester", "kind": "delegated", "state": "blocked"}]}),
            json!({"id": "w", "agent_id": "worker", "parent": "d", "body": "done\n0 1 1 2"}),
            json!({"id": "s", "agent_id": "lead", "parent": "d", "body": "worker finished, tester blocked"}),
        ];
        assert_eq!(
            tree(&messages),
            "you: fib please\n   lead: [task for worker] write fib.py…\n      tester: ! blocked\n      worker: done…\n      lead (summary): worker finished, tester blocked\n\n"
        );
    }
}
