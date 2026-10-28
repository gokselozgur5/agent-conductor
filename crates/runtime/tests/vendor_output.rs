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
