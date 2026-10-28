use super::*;

fn receipt() -> Value {
    json!({"check_id":"check", "source_sha256":"a".repeat(64),
        "definition_sha256":"b".repeat(64),"outcome":"passed","exit_code":0,
        "stdout_sha256":"c".repeat(64),"stderr_sha256":null,
        "unexpected_private_output":"DO_NOT_EXPOSE"})
}
fn row(kind: &str, reason: &Value) -> EvidenceRow {
    let command = json!({"actor":{"id":"conductor-check-executor","role":"system"},
        "expected_version":9_007_199_254_740_993_u64,"correlation_id":"PRIVATE_CORRELATION",
        "idempotency_key":"PRIVATE_KEY","reason":reason.to_string(),
        "kind":{"type":kind,"check_id":"check","passed":true,"source":format!("snapshot:{}","a".repeat(64))}});
    EvidenceRow {
        sequence: 1,
        kind: kind.into(),
        actor_id: "conductor-check-executor".into(),
        occurred_at_ms: 1,
        payload: Some(json!(["task", command]).to_string()),
        execution: None,
    }
}
#[test]
fn check_receipts_require_matching_source_check_outcome_and_runtime_actor() {
    let base = row("record_check", &receipt());
    let projected = project("task", &base).unwrap();
    assert_eq!(projected["receipt_status"], "recorded_runtime_receipt");
    assert_eq!(projected["task_version_before"], "9007199254740993");
    for secret in ["DO_NOT_EXPOSE", "PRIVATE_CORRELATION", "PRIVATE_KEY"] {
        assert!(!projected.to_string().contains(secret));
    }
    for (field, value) in [
        ("check_id", json!("other")),
        ("source_sha256", json!("d".repeat(64))),
        ("exit_code", json!(1)),
        ("outcome", json!("failed")),
        ("definition_sha256", json!("bad")),
        ("stdout_sha256", json!("bad")),
    ] {
        let mut invalid = receipt();
        invalid[field] = value;
        let projected = project("task", &row("record_check", &invalid)).unwrap();
        assert_eq!(projected["receipt_status"], "unavailable");
        assert!(projected.get("runtime_receipt").is_none());
    }
    let mut untrusted = base;
    let mut payload: Value = serde_json::from_str(untrusted.payload.as_deref().unwrap()).unwrap();
    payload[1]["actor"]["role"] = json!("operator");
    untrusted.payload = Some(payload.to_string());
    assert_eq!(
        project("task", &untrusted).unwrap()["receipt_status"],
        "unavailable"
    );
    assert!(project("other-task", &untrusted).is_none());
    untrusted.actor_id = "other-actor".into();
    assert!(project("task", &untrusted).is_none());
}
#[test]
fn missing_malformed_and_nonstructured_evidence_is_not_promoted_to_proof() {
    let mut value = row("record_check", &json!("not a receipt"));
    assert_eq!(
        project("task", &value).unwrap()["receipt_status"],
        "unavailable"
    );
    value.payload = Some("{}".into());
    assert!(project("task", &value).is_none());
    value.payload = None;
    assert!(project("task", &value).is_none());
    value.kind = "execution.exited".into();
    value.payload = Some("{}".into());
    assert!(
        project("task", &value).is_none(),
        "missing execution link must stay unavailable"
    );
}
#[test]
fn historical_revision_keeps_integer_precision_separate_from_current_state() {
    let value = EvidenceRow {
        sequence: 1,
        kind: "execution.exited".into(),
        actor_id: "runtime".into(),
        occurred_at_ms: 1,
        payload: Some(json!({"revision":9_007_199_254_740_993_i64}).to_string()),
        execution: Some(
            json!({"current_recorded_state":"fenced","current_revision":"9007199254740994"})
                .to_string(),
        ),
    };
    let projected = project("task", &value).unwrap();
    assert_eq!(projected["event_revision"], "9007199254740993");
    assert_eq!(
        projected["current_execution_snapshot"]["current_recorded_state"],
        "fenced"
    );
}
#[test]
fn review_projection_omits_explanation_and_rejects_malformed_digest() {
    let mut value = row(
        "record_verdict",
        &json!({"explanation":"PRIVATE_EXPLANATION","output_sha256":"d".repeat(64)}),
    );
    let mut payload: Value = serde_json::from_str(value.payload.as_deref().unwrap()).unwrap();
    payload[1]["kind"] =
        json!({"type":"record_verdict","accepted":false,"source":"snapshot:source"});
    value.payload = Some(payload.to_string());
    let projected = project("task", &value).unwrap();
    assert_eq!(projected["accepted"], false);
    assert_eq!(projected["review_output_sha256"], "d".repeat(64));
    assert!(!projected.to_string().contains("PRIVATE_EXPLANATION"));
    payload[1]["reason"] = json!("{\"output_sha256\":\"not a digest\"}");
    value.payload = Some(payload.to_string());
    assert_eq!(
        project("task", &value).unwrap()["output_digest_status"],
        "unavailable"
    );
}
