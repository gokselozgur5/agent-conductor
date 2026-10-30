//! A bounded delegation proposal, validated against operator-owned room authority.
//! Parsing has no side effects and grants no native launch or task acceptance.
use crate::ConversationRoom;
use agent_conductor_store::StoreError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const SCHEMA: &str = "conductor.delegation.v1";
const MAX_BODY_BYTES: usize = 32_768;
const MAX_ASSIGNMENTS: usize = 8;
const MAX_INSTRUCTION_BYTES: usize = 4_096;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationAssignment {
    pub agent_id: String,
    pub instruction: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationPlan {
    pub schema: String,
    pub assignments: Vec<DelegationAssignment>,
}

/// Parses one complete JSON proposal using an author obtained from stored native
/// provenance. No author, root request or parent identity comes from this JSON.
/// The bounds apply only to this proposal, not to a durable execution tree.
/// # Errors
/// Rejects unsupported schemas, extra fields, invalid sizes, duplicate recipients
/// and assignments outside the stored author's direct reporting authority.
pub fn parse_delegation(
    body: &str,
    room: &ConversationRoom,
    stored_author: &str,
) -> Result<DelegationPlan, StoreError> {
    if body.len() > MAX_BODY_BYTES {
        return Err(invalid("delegation proposal exceeds input bound"));
    }
    let plan: DelegationPlan = serde_json::from_str(body)
        .map_err(|_| invalid("delegation proposal must be one strict JSON object"))?;
    if plan.schema != SCHEMA {
        return Err(invalid("unsupported delegation schema"));
    }
    if plan.assignments.is_empty() || plan.assignments.len() > MAX_ASSIGNMENTS {
        return Err(invalid(
            "delegation requires between one and eight assignments",
        ));
    }
    let mut recipients = BTreeSet::new();
    for assignment in &plan.assignments {
        if assignment.instruction.trim().is_empty()
            || assignment.instruction.len() > MAX_INSTRUCTION_BYTES
            || !recipients.insert(&assignment.agent_id)
        {
            return Err(invalid("invalid or duplicate delegation assignment"));
        }
        room.delegate(stored_author, &assignment.agent_id)?;
    }
    Ok(plan)
}

fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RoomMember;

    fn room() -> ConversationRoom {
        let mut members = vec![RoomMember {
            agent_id: "lead".into(),
            reports_to: None,
            observes: BTreeSet::new(),
        }];
        for index in 0..9 {
            members.push(RoomMember {
                agent_id: format!("worker{index}"),
                reports_to: Some("lead".into()),
                observes: BTreeSet::new(),
            });
        }
        let configured = members.iter().map(|m| m.agent_id.clone()).collect();
        ConversationRoom::new("lead".into(), members, &configured).unwrap()
    }

    fn proposal(target: &str, instruction: &str) -> String {
        serde_json::json!({
            "schema": SCHEMA,
            "assignments": [{"agent_id": target, "instruction": instruction}],
        })
        .to_string()
    }

    #[test]
    fn parses_direct_assignment_and_preserves_instruction_bytes() {
        let instruction = " Inspect parser failures.\nReport evidence. ";
        let plan = parse_delegation(&proposal("worker0", instruction), &room(), "lead").unwrap();
        assert_eq!(plan.schema, SCHEMA);
        assert_eq!(plan.assignments.len(), 1);
        assert_eq!(plan.assignments[0].agent_id, "worker0");
        assert_eq!(plan.assignments[0].instruction, instruction);
    }

    #[test]
    fn requires_a_complete_supported_json_document() {
        let valid = proposal("worker0", "Inspect parser");
        for body in [
            format!("Here is the plan: {valid}"),
            format!("```json\n{valid}\n```"),
            format!("{valid} {{}}"),
            valid.replace(SCHEMA, "conductor.delegation.v2"),
            valid.trim_end_matches('}').to_string(),
            "[]".into(),
            "null".into(),
        ] {
            assert!(parse_delegation(&body, &room(), "lead").is_err());
        }
    }

    #[test]
    fn refuses_spoofed_identity_and_unknown_assignment_fields() {
        let original: serde_json::Value =
            serde_json::from_str(&proposal("worker0", "Inspect parser")).unwrap();
        for field in [
            "author",
            "stored_author",
            "root_id",
            "parent_id",
            "conversation_id",
        ] {
            let mut value = original.clone();
            value[field] = "lead".into();
            assert!(parse_delegation(&value.to_string(), &room(), "lead").is_err());
            let mut value = original.clone();
            value["assignments"][0][field] = "lead".into();
            assert!(parse_delegation(&value.to_string(), &room(), "lead").is_err());
        }
        let repeated = r#"{"schema":"conductor.delegation.v1","schema":"conductor.delegation.v1","assignments":[{"agent_id":"worker0","instruction":"Inspect"}]}"#;
        assert!(parse_delegation(repeated, &room(), "lead").is_err());
    }

    #[test]
    fn validates_the_stored_authors_direct_reporting_authority() {
        for (author, target) in [
            ("worker0", "worker1"),
            ("worker0", "lead"),
            ("lead", "lead"),
            ("outsider", "worker0"),
            ("lead", "outsider"),
            ("lead", ""),
        ] {
            assert!(parse_delegation(&proposal(target, "Inspect"), &room(), author).is_err());
        }
    }

    #[test]
    fn rejects_duplicate_empty_and_excessive_assignments() {
        let assignment = serde_json::json!({"agent_id":"worker0","instruction":"Inspect"});
        let duplicate =
            serde_json::json!({"schema":SCHEMA,"assignments":[assignment.clone(),assignment]});
        assert!(parse_delegation(&duplicate.to_string(), &room(), "lead").is_err());
        for count in [0, 8, 9] {
            let assignments: Vec<_> = (0..count)
                .map(|i| serde_json::json!({"agent_id":format!("worker{i}"),"instruction":"Inspect"}))
                .collect();
            let body = serde_json::json!({"schema":SCHEMA,"assignments":assignments}).to_string();
            assert_eq!(parse_delegation(&body, &room(), "lead").is_ok(), count == 8);
        }
    }

    #[test]
    fn enforces_utf8_byte_bounds_and_rejects_blank_instructions() {
        for instruction in [
            String::new(),
            " \n\t".into(),
            "a".repeat(4_097),
            "é".repeat(2_049),
        ] {
            assert!(parse_delegation(&proposal("worker0", &instruction), &room(), "lead").is_err());
        }
        assert!(
            parse_delegation(&proposal("worker0", &"é".repeat(2_048)), &room(), "lead").is_ok()
        );
        let valid = proposal("worker0", "Inspect");
        let at_limit = format!("{valid}{}", " ".repeat(MAX_BODY_BYTES - valid.len()));
        assert!(parse_delegation(&at_limit, &room(), "lead").is_ok());
        assert!(parse_delegation(&(at_limit + " "), &room(), "lead").is_err());
    }
}
