//! Optional vendor-reported accounting claims, never billing or acceptance truth.
use crate::AdapterKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportedUsage {
    pub status: String,
    pub terminal_outcome: String,
    /// Decimal strings preserve exact integers through JavaScript consumers.
    pub tokens: BTreeMap<String, String>,
    pub reported_cost_usd: Option<String>,
    pub reported_model: Option<String>,
    pub invalid_fields: Vec<String>,
}
impl Default for ReportedUsage {
    fn default() -> Self {
        Self {
            status: "unavailable".into(),
            terminal_outcome: "unknown".into(),
            tokens: BTreeMap::new(),
            reported_cost_usd: None,
            reported_model: None,
            invalid_fields: Vec::new(),
        }
    }
}

/// Reads only allowlisted fields from a unique final top-level terminal event.
/// Missing, malformed or interrupted reports never become zero-valued usage.
#[must_use]
pub fn reported_usage(kind: AdapterKind, stdout: &[u8]) -> ReportedUsage {
    let mut report = ReportedUsage::default();
    if stdout.len() > 2 * 1024 * 1024 || stdout.last() != Some(&b'\n') {
        report.status = "incomplete".into();
        return report;
    }
    let mut terminal = None;
    for (count, line) in stdout
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .enumerate()
    {
        if line.len() > 512 * 1024 || count >= 10_000 || terminal.is_some() {
            report.status = "ambiguous".into();
            return report;
        }
        let Ok(event) = serde_json::from_slice::<Value>(line) else {
            report.status = "invalid".into();
            return report;
        };
        if !event.is_object() {
            report.status = "invalid".into();
            return report;
        }
        let event_type = event.get("type").and_then(Value::as_str);
        if kind == AdapterKind::Claude
            && event_type == Some("system")
            && event.get("subtype").and_then(Value::as_str) == Some("init")
        {
            report.reported_model = event
                .get("model")
                .and_then(Value::as_str)
                .filter(|s| {
                    !s.is_empty()
                        && s.len() <= 128
                        && !s.starts_with("sk-")
                        && s.bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b"-._/:".contains(&b))
                })
                .map(str::to_owned);
        }
        if (kind == AdapterKind::Claude && event_type == Some("result"))
            || (kind == AdapterKind::Codex
                && matches!(event_type, Some("turn.completed" | "turn.failed")))
        {
            terminal = Some(event);
        }
    }
    let Some(event) = terminal else {
        report.status = "incomplete".into();
        return report;
    };
    finish_report(kind, &event, report)
}

fn finish_report(kind: AdapterKind, event: &Value, mut report: ReportedUsage) -> ReportedUsage {
    report.terminal_outcome = match kind {
        AdapterKind::Claude
            if event.get("subtype").and_then(Value::as_str) == Some("success")
                && event.get("is_error").and_then(Value::as_bool) == Some(false) =>
        {
            "success"
        }
        AdapterKind::Codex
            if event.get("type").and_then(Value::as_str) == Some("turn.completed") =>
        {
            "success"
        }
        AdapterKind::Claude | AdapterKind::Codex => "failure",
        AdapterKind::Fake => "unknown",
    }
    .into();
    if let Some(usage) = event.get("usage") {
        if usage.is_object() {
            let fields: &[&str] = match kind {
                AdapterKind::Claude => &[
                    "input_tokens",
                    "output_tokens",
                    "cache_read_input_tokens",
                    "cache_creation_input_tokens",
                ],
                AdapterKind::Codex => &["input_tokens", "output_tokens", "cached_input_tokens"],
                AdapterKind::Fake => &[],
            };
            for field in fields {
                if let Some(value) = usage.get(field) {
                    if let Some(number) = value.as_u64() {
                        report.tokens.insert((*field).into(), number.to_string());
                    } else {
                        report.invalid_fields.push((*field).into());
                    }
                }
            }
        } else {
            report.invalid_fields.push("usage".into());
        }
    }
    if kind == AdapterKind::Claude
        && let Some(value) = event.get("total_cost_usd")
    {
        if value.is_number()
            && value
                .as_f64()
                .is_some_and(|v| v.is_finite() && (0.0..=1_000_000_000.0).contains(&v))
        {
            report.reported_cost_usd = Some(value.to_string());
        } else {
            report.invalid_fields.push("total_cost_usd".into());
        }
    }
    let present = !report.tokens.is_empty() || report.reported_cost_usd.is_some();
    report.status = match (present, report.invalid_fields.is_empty()) {
        (true, true) => "reported",
        (true, false) => "partial",
        (false, true) => "missing",
        (false, false) => "invalid",
    }
    .into();
    report
}
