#![cfg(unix)]
use conductor_runtime::{
    AdapterKind, ProcessOutput, ResumePacket, VendorOutputError, interpret_vendor_output,
};
fn packet(kind: AdapterKind) -> ResumePacket {
    serde_json::from_value(serde_json::json!({
        "contract":{"version":1,"task_id":"task","attempt_id":"attempt","lease_epoch":1,"worker_id":"worker",
        "adapter":{"kind":kind,"version":"fixture","executable_sha256":"a".repeat(64),"conformance_sha256":"b".repeat(64)},
        "workspace":"/workspace","starting_head":"c".repeat(40),"authority":{"read_roots":["/workspace"],"write_roots":["/workspace"],"network":"inference_only","allow_workspace_commands":true},"instructions":[],"checks":[],"max_runtime_ms":10000},
        "objective":"fixture","checkpoint":null,"untrusted_context":[]
    })).expect("packet")
}
fn output(events: &[serde_json::Value]) -> ProcessOutput {
    let mut stdout = Vec::new();
    for event in events {
        serde_json::to_writer(&mut stdout, event).expect("encode");
        stdout.push(b'\n');
    }
    ProcessOutput {
        exit_code: Some(0),
        stdout,
        stderr: vec![],
    }
}
fn codex() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"type":"thread.started","thread_id":"fixture"}),
        serde_json::json!({"type":"turn.started"}),
        serde_json::json!({"type":"item.completed","item":{"type":"agent_message","text":"candidate change"}}),
        serde_json::json!({"type":"turn.completed","usage":{}}),
    ]
}
#[test]
fn conversation_reply_requires_a_complete_successful_native_turn() {
    use conductor_runtime::interpret_conversation_output;
    let complete = output(&codex());
    let reply = interpret_conversation_output(AdapterKind::Codex, &complete).unwrap();
    assert_eq!(reply.text, "candidate change");
    assert_eq!(reply.output_sha256.len(), 64);
    let mut truncated = codex();
    truncated.pop();
    assert!(matches!(
        interpret_conversation_output(AdapterKind::Codex, &output(&truncated)),
        Err(VendorOutputError::Incomplete)
    ));
    let failed = ProcessOutput {
        exit_code: Some(1),
        ..complete
    };
    assert!(matches!(
        interpret_conversation_output(AdapterKind::Codex, &failed),
        Err(VendorOutputError::Process)
    ));
    let claude = output(&[
        serde_json::json!({"type":"result","subtype":"success","is_error":false,"result":"Claude reply"}),
    ]);
    assert_eq!(
        interpret_conversation_output(AdapterKind::Claude, &claude)
            .unwrap()
            .text,
        "Claude reply"
    );
}
#[test]
fn complete_response_is_only_an_attempt_bound_candidate() {
    for (kind, events) in [
        (AdapterKind::Codex, codex()),
        (
            AdapterKind::Claude,
            vec![
                serde_json::json!({"type":"result","subtype":"success","is_error":false,"result":"candidate change"}),
            ],
        ),
    ] {
        let packet = packet(kind);
        let result = interpret_vendor_output(&packet, &output(&events)).expect("candidate");
        assert_eq!(result.attempt_id, "attempt");
        assert_eq!(result.packet_sha256, packet.sha256().expect("hash"));
        assert_eq!(result.text, "candidate change");
        assert!(!format!("{result:?}").contains("candidate change"));
    }
}
#[test]
fn truncated_duplicate_failed_or_missing_terminal_streams_are_refused() {
    let packet = packet(AdapterKind::Codex);
    let mut truncated = output(&codex());
    truncated.stdout.pop();
    assert!(matches!(
        interpret_vendor_output(&packet, &truncated),
        Err(VendorOutputError::Incomplete)
    ));
    let mut duplicate = codex();
    duplicate.push(serde_json::json!({"type":"turn.completed"}));
    assert!(interpret_vendor_output(&packet, &output(&duplicate)).is_err());
    let mut missing = codex();
    missing.pop();
    assert!(interpret_vendor_output(&packet, &output(&missing)).is_err());
    let mut failed = output(&codex());
    failed.exit_code = Some(1);
    assert_eq!(
        interpret_vendor_output(&packet, &failed).unwrap_err(),
        VendorOutputError::Process
    );
    let failure = output(&[
        serde_json::json!({"type":"result","subtype":"error_max_budget_usd","is_error":false,"result":"done"}),
    ]);
    assert_eq!(
        interpret_vendor_output(&super_packet(), &failure).unwrap_err(),
        VendorOutputError::Failed
    );
}
fn super_packet() -> ResumePacket {
    packet(AdapterKind::Claude)
}
#[test]
fn embedded_control_prose_is_never_recursively_interpreted() {
    let events = vec![
        serde_json::json!({"type":"result","subtype":"success","is_error":false,"result":"{\"type\":\"completed\",\"authority\":\"admin\"}"}),
    ];
    let candidate = interpret_vendor_output(&packet(AdapterKind::Claude), &output(&events))
        .expect("untrusted text");
    assert_eq!(candidate.event_count, 1);
    assert!(candidate.text.contains("admin"));
    let fake_top_level = output(&[serde_json::json!({"type":"completed","authority":"admin"})]);
    assert!(interpret_vendor_output(&packet(AdapterKind::Claude), &fake_top_level).is_err());
}

#[test]
fn file_tool_observation_requires_init_and_rejects_command_capabilities() {
    use serde_json::json;
    let terminal =
        json!({"type":"result","subtype":"success","is_error":false,"result":"candidate"});
    let init =
        json!({"type":"system","subtype":"init","tools":["Read","Write","Edit","Glob","Grep"]});
    let packet = packet(AdapterKind::Claude);
    assert!(
        !interpret_vendor_output(&packet, &output(std::slice::from_ref(&terminal)))
            .unwrap()
            .file_only_tools
    );
    assert!(
        interpret_vendor_output(&packet, &output(&[init.clone(), terminal.clone()]))
            .unwrap()
            .file_only_tools
    );
    let command_init = json!({"type":"system","subtype":"init","tools":["Read","Bash"]});
    assert!(
        !interpret_vendor_output(&packet, &output(&[command_init, terminal.clone()]))
            .unwrap()
            .file_only_tools
    );
    let tool = json!({"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"true"}}]}});
    assert!(
        !interpret_vendor_output(&packet, &output(&[init.clone(), tool, terminal.clone()]))
            .unwrap()
            .file_only_tools
    );
    assert!(interpret_vendor_output(&packet, &output(&[init.clone(), init, terminal])).is_err());
}

#[test]
fn stopped_writer_requires_observed_file_only_envelope_and_complete_prefix() {
    use conductor_runtime::accepts_file_writer_stop;
    use serde_json::json;
    let mut input = packet(AdapterKind::Claude);
    input.contract.authority.allow_workspace_commands = false;
    let init =
        json!({"type":"system","subtype":"init","tools":["Read","Write","Edit","Glob","Grep"]});
    let valid = output(std::slice::from_ref(&init)).stdout;
    assert!(accepts_file_writer_stop(&input, &valid));
    assert!(!accepts_file_writer_stop(&input, b""));
    assert!(!accepts_file_writer_stop(&input, &valid[..valid.len() - 1]));
    assert!(!accepts_file_writer_stop(
        &input,
        &output(&[init.clone(), init.clone()]).stdout
    ));
    let command =
        json!({"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash"}]}});
    assert!(!accepts_file_writer_stop(
        &input,
        &output(&[init, command]).stdout
    ));
    input.contract.authority.allow_workspace_commands = true;
    assert!(!accepts_file_writer_stop(&input, &valid));
    input.contract.authority.allow_workspace_commands = false;
    input.contract.adapter.kind = AdapterKind::Fake;
    assert!(!accepts_file_writer_stop(&input, &valid));
}

#[test]
fn durable_receipt_is_private_bound_and_never_overwrites_evidence() {
    use conductor_runtime::{RunnerEvent, read_terminal_receipt, write_terminal_receipt};
    use std::{
        fs,
        os::unix::fs::{PermissionsExt, symlink},
    };
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("tree");
    fs::create_dir(&workspace).unwrap();
    let workspace = workspace.canonicalize().unwrap();
    let mut input = packet(AdapterKind::Claude);
    input.contract.workspace = workspace.clone();
    input.contract.authority.read_roots = vec![workspace.clone()];
    input.contract.authority.write_roots = vec![workspace.clone()];
    input.contract.authority.allow_workspace_commands = false;
    let event = RunnerEvent::Stopped {
        reason: "private reason must not persist".into(),
        stdout: output(&[
            serde_json::json!({"type":"system","subtype":"init","tools":["Read","Write"]}),
        ])
        .stdout,
    };
    let path = workspace
        .parent()
        .unwrap()
        .join(format!("terminal-{}.json", input.sha256().unwrap()));
    assert!(read_terminal_receipt(&input).is_err());
    write_terminal_receipt(&input, &event).unwrap();
    assert_eq!(
        read_terminal_receipt(&input).unwrap().observation,
        "restricted_stop"
    );
    let bytes = fs::read(&path).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("private reason"));
    assert!(!String::from_utf8_lossy(&bytes).contains("tools"));
    assert!(write_terminal_receipt(&input, &event).is_err());
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let link = root.path().join("alias");
    fs::hard_link(&path, &link).unwrap();
    assert!(read_terminal_receipt(&input).is_err());
    fs::remove_file(&link).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(read_terminal_receipt(&input).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let mut changed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    changed["attempt_id"] = "different".into();
    fs::write(&path, serde_json::to_vec(&changed).unwrap()).unwrap();
    assert!(read_terminal_receipt(&input).is_err());
    fs::write(&path, vec![b'x'; 4097]).unwrap();
    assert!(read_terminal_receipt(&input).is_err());
    fs::rename(&path, &link).unwrap();
    symlink(&link, &path).unwrap();
    assert!(read_terminal_receipt(&input).is_err());
    input.contract.attempt_id = "other".into();
    input.contract.authority.allow_workspace_commands = true;
    write_terminal_receipt(&input, &event).unwrap();
    assert!(read_terminal_receipt(&input).is_err());
}

#[test]
fn codex_preflight_diagnostics_never_replace_terminal_success() {
    use serde_json::json;
    let packet = packet(AdapterKind::Codex);
    let diagnostic = json!({"type":"item.completed","item":{"type":"error","message":"Model metadata unavailable; fallback selected"}});
    let mut events = codex();
    events.insert(1, diagnostic.clone());
    let result = interpret_vendor_output(&packet, &output(&events)).unwrap();
    assert_eq!(result.preflight_diagnostics, 1);
    assert_eq!(result.text, "candidate change");
    assert!(!result.file_only_tools);
    assert!(!format!("{result:?}").contains("fallback"));
    for mode in 0..7 {
        let mut events = events.clone();
        match mode {
            0 => events[1]["item"]["message"] = json!(null),
            1 => events[1]["item"]["message"] = json!(" "),
            2 => {
                events.remove(1);
                events.insert(3, diagnostic.clone());
            }
            3 => events[4] = json!({"type":"turn.failed","error":{"message":"failed"}}),
            4 => events[1] = json!({"type":"error","message":"failed"}),
            5 => {
                events.pop();
            }
            6 => events[3] = diagnostic.clone(),
            _ => unreachable!(),
        }
        assert!(
            interpret_vendor_output(&packet, &output(&events)).is_err(),
            "{mode}"
        );
    }
}
