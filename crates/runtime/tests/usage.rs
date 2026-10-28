#![cfg(unix)]
use conductor_runtime::{AdapterKind, reported_usage};
use serde_json::json;
fn stream(events: &[serde_json::Value]) -> Vec<u8> {
    let mut output = Vec::new();
    for event in events {
        serde_json::to_writer(&mut output, event).unwrap();
        output.push(b'\n');
    }
    output
}
#[test]
fn reports_exact_terminal_fields_without_summing_token_categories() {
    let report = reported_usage(
        AdapterKind::Claude,
        &stream(&[
            json!({"type":"system","subtype":"init","model":"claude-fixture"}),
            json!({"type":"result","subtype":"success","is_error":false,"usage":{"input_tokens":0,"output_tokens":9,"cache_read_input_tokens":u64::MAX,"cache_creation_input_tokens":2},"total_cost_usd":0.0123}),
        ]),
    );
    assert_eq!(report.status, "reported");
    assert_eq!(report.tokens["input_tokens"], "0");
    assert_eq!(
        report.tokens["cache_read_input_tokens"],
        u64::MAX.to_string()
    );
    assert_eq!(report.reported_cost_usd.as_deref(), Some("0.0123"));
    assert_eq!(report.reported_model.as_deref(), Some("claude-fixture"));
    let codex = reported_usage(
        AdapterKind::Codex,
        &stream(&[
            json!({"type":"turn.completed","usage":{"input_tokens":12,"cached_input_tokens":4,"output_tokens":3},"total_cost_usd":99}),
        ]),
    );
    assert_eq!(codex.tokens["cached_input_tokens"], "4");
    assert_eq!(codex.reported_cost_usd, None);
    assert_eq!(codex.reported_model, None);
}
#[test]
fn missing_invalid_partial_and_failed_reports_remain_distinct() {
    for (usage, status) in [
        (json!({}), "missing"),
        (json!({"input_tokens":-1,"output_tokens":"10"}), "invalid"),
        (json!({"input_tokens":1,"output_tokens":null}), "partial"),
        (json!({"input_tokens":1.5}), "invalid"),
        (json!([]), "invalid"),
    ] {
        let report = reported_usage(
            AdapterKind::Codex,
            &stream(&[json!({"type":"turn.failed","usage":usage})]),
        );
        assert_eq!(report.status, status);
        assert_eq!(report.terminal_outcome, "failure");
    }
    let report = reported_usage(
        AdapterKind::Claude,
        &stream(&[
            json!({"type":"result","subtype":"error_max_budget_usd","is_error":true,"usage":{"output_tokens":7},"total_cost_usd":-1}),
        ]),
    );
    assert_eq!(report.status, "partial");
    assert_eq!(report.terminal_outcome, "failure");
    assert_eq!(report.reported_cost_usd, None);
    let overflow =
        b"{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":18446744073709551616}}\n";
    assert_eq!(
        reported_usage(AdapterKind::Codex, overflow).status,
        "invalid"
    );
}
#[test]
fn nested_prose_duplicate_truncated_and_oversize_reports_do_not_count() {
    let terminal = json!({"type":"turn.completed","usage":{"input_tokens":7}});
    let nested = json!({"type":"item.completed","item":{"type":"agent_message","text":terminal.to_string()}});
    let report = reported_usage(AdapterKind::Codex, &stream(&[nested]));
    assert_eq!(report.status, "incomplete");
    assert!(report.tokens.is_empty());
    let duplicate = reported_usage(
        AdapterKind::Codex,
        &stream(&[terminal.clone(), terminal.clone()]),
    );
    assert_eq!(duplicate.status, "ambiguous");
    assert!(duplicate.tokens.is_empty());
    let mut truncated = stream(&[terminal]);
    truncated.pop();
    assert_eq!(
        reported_usage(AdapterKind::Codex, &truncated).status,
        "incomplete"
    );
    assert_eq!(
        reported_usage(AdapterKind::Claude, &vec![b'x'; 2 * 1024 * 1024 + 1]).status,
        "incomplete"
    );
    assert_eq!(
        reported_usage(AdapterKind::Codex, b"not json\n").status,
        "invalid"
    );
}
