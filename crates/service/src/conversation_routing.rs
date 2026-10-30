//! Command authority and information routing are separate room relationships.
use std::collections::{BTreeMap, BTreeSet};

use agent_conductor_store::StoreError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RoomMember {
    pub agent_id: String,
    /// None means this agent reports directly to the operator.
    pub reports_to: Option<String>,
    /// Agents whose results this member needs, independent of reporting lines.
    pub observes: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoomDeliveryKind {
    Assignment,
    Information,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct RoomRecipient {
    pub agent_id: String,
    pub kind: RoomDeliveryKind,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RoomConfiguration {
    pub coordinator: String,
    pub members: Vec<RoomMember>,
}

/// Validated operator-owned topology. Model prose cannot change this structure.
#[derive(Clone, Debug)]
pub struct ConversationRoom {
    coordinator: String,
    members: BTreeMap<String, RoomMember>,
}

impl ConversationRoom {
    /// Validates known membership, dependency references and acyclic reporting.
    /// Information subscriptions may be cyclic: they never authorize execution.
    /// # Errors
    /// Rejects unknown agents, duplicate members, self-links and command cycles.
    pub fn new(
        coordinator: String,
        members: Vec<RoomMember>,
        configured_agents: &BTreeSet<String>,
    ) -> Result<Self, StoreError> {
        let mut indexed = BTreeMap::new();
        if members.is_empty() || members.len() > 32 {
            return Err(invalid("room requires between one and 32 members"));
        }
        for member in members {
            if !configured_agents.contains(&member.agent_id)
                || indexed.insert(member.agent_id.clone(), member).is_some()
            {
                return Err(invalid("unknown or duplicate room member"));
            }
        }
        if !indexed.contains_key(&coordinator) {
            return Err(invalid("room coordinator must be a member"));
        }
        for member in indexed.values() {
            for reference in member.reports_to.iter().chain(&member.observes) {
                if reference == &member.agent_id || !indexed.contains_key(reference) {
                    return Err(invalid("invalid room relationship"));
                }
            }
            let mut seen = BTreeSet::new();
            let mut cursor = Some(&member.agent_id);
            while let Some(id) = cursor {
                if !seen.insert(id) {
                    return Err(invalid("command hierarchy contains a cycle"));
                }
                cursor = indexed.get(id).and_then(|m| m.reports_to.as_ref());
            }
        }
        Ok(Self {
            coordinator,
            members: indexed,
        })
    }

    /// Persists operator addressing with the actual stored message as authority.
    /// # Errors
    /// Rejects forged agent-to-operator promotion and conflicting route replay.
    pub async fn route_operator_message(
        &self,
        store: &agent_conductor_store::Store,
        message_id: &str,
        recipients: &[String],
        everyone: bool,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let message = store
            .conversation_message(message_id)
            .await?
            .ok_or_else(|| invalid("room message missing"))?;
        if message.sender != "operator" {
            return Err(invalid("operator message required for room command"));
        }
        self.persist_route(
            store,
            message_id,
            self.operator_request(recipients, everyone)?,
            now_ms,
        )
        .await
    }

    /// Automatically informs relevant members from a recorded native reply.
    /// The author comes from storage, never from a model-supplied routing field.
    /// # Errors
    /// Rejects non-agent messages, unknown authors and changed route replay.
    pub async fn route_agent_update(
        &self,
        store: &agent_conductor_store::Store,
        message_id: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let message = store
            .conversation_message(message_id)
            .await?
            .ok_or_else(|| invalid("room message missing"))?;
        if message.sender != "agent" {
            return Err(invalid("recorded agent reply required for room update"));
        }
        let author = message
            .agent_id
            .as_deref()
            .ok_or_else(|| invalid("room update author missing"))?;
        self.persist_route(store, message_id, self.inform(author)?, now_ms)
            .await
    }

    async fn persist_route(
        &self,
        store: &agent_conductor_store::Store,
        message_id: &str,
        recipients: Vec<RoomRecipient>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let recipients: Vec<_> = recipients
            .into_iter()
            .map(|r| agent_conductor_store::ConversationRecipient {
                agent_id: r.agent_id,
                kind: match r.kind {
                    RoomDeliveryKind::Assignment => "assignment",
                    RoomDeliveryKind::Information => "information",
                }
                .into(),
                reason: r.reason,
            })
            .collect();
        store
            .route_conversation_message(message_id, &recipients, now_ms)
            .await
    }

    /// Routes an operator request to explicit recipients, all members, or the
    /// coordinator by default. Explicit routing is separate from message prose.
    /// # Errors
    /// Rejects ambiguous or unknown recipients instead of silently rerouting.
    pub fn operator_request(
        &self,
        recipients: &[String],
        everyone: bool,
    ) -> Result<Vec<RoomRecipient>, StoreError> {
        if everyone && !recipients.is_empty() {
            return Err(invalid("room-wide and individual addressing conflict"));
        }
        let ids: BTreeSet<String> = if everyone {
            self.members.keys().cloned().collect()
        } else if recipients.is_empty() {
            BTreeSet::from([self.coordinator.clone()])
        } else {
            recipients.iter().cloned().collect()
        };
        if ids.iter().any(|id| !self.members.contains_key(id)) {
            return Err(invalid("unknown room recipient"));
        }
        Ok(ids
            .into_iter()
            .map(|agent_id| RoomRecipient {
                agent_id,
                kind: RoomDeliveryKind::Assignment,
                reason: "operator_request".into(),
            })
            .collect())
    }

    /// Checks direct reporting authority for a structured delegation request.
    /// # Errors
    /// Rejects attempts to command peers, ancestors or unknown agents.
    pub fn delegate(&self, source: &str, target: &str) -> Result<RoomRecipient, StoreError> {
        if !self.members.contains_key(source)
            || self
                .members
                .get(target)
                .and_then(|m| m.reports_to.as_deref())
                != Some(source)
        {
            return Err(invalid(
                "delegation requires a direct reporting relationship",
            ));
        }
        Ok(RoomRecipient {
            agent_id: target.into(),
            kind: RoomDeliveryKind::Assignment,
            reason: "delegated_by_supervisor".into(),
        })
    }

    /// A result informs the supervisor and interested members, once each. It
    /// does not recursively forward notifications or create new assignments.
    /// # Errors
    /// Rejects an author outside this room.
    pub fn inform(&self, source: &str) -> Result<Vec<RoomRecipient>, StoreError> {
        let member = self
            .members
            .get(source)
            .ok_or_else(|| invalid("unknown room author"))?;
        let mut targets = BTreeMap::new();
        if let Some(supervisor) = &member.reports_to {
            targets.insert(supervisor.clone(), "report_to_supervisor");
        }
        for observer in self.members.values() {
            if observer.observes.contains(source) {
                targets
                    .entry(observer.agent_id.clone())
                    .or_insert("dependency_update");
            }
        }
        Ok(targets
            .into_iter()
            .map(|(agent_id, reason)| RoomRecipient {
                agent_id,
                kind: RoomDeliveryKind::Information,
                reason: reason.into(),
            })
            .collect())
    }
}

fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, supervisor: Option<&str>, observes: &[&str]) -> RoomMember {
        RoomMember {
            agent_id: id.into(),
            reports_to: supervisor.map(Into::into),
            observes: observes.iter().map(|s| (*s).into()).collect(),
        }
    }

    fn room() -> ConversationRoom {
        ConversationRoom::new(
            "lead".into(),
            vec![
                member("lead", None, &["coder"]),
                member("coder", Some("lead"), &[]),
                member("reviewer", Some("lead"), &["coder"]),
            ],
            &BTreeSet::from(["lead".into(), "coder".into(), "reviewer".into()]),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn stored_authorship_controls_durable_routing() {
        let temporary = tempfile::tempdir().unwrap();
        let store = agent_conductor_store::Store::open(temporary.path().join("room.sqlite3"))
            .await
            .unwrap();
        store
            .create_conversation("room", "/project", 1)
            .await
            .unwrap();
        for (id, sender, agent) in [
            ("request", "operator", None),
            ("result", "agent", Some("coder")),
        ] {
            store
                .append_conversation_message(
                    &agent_conductor_store::ConversationMessage {
                        sequence: 0,
                        id: id.into(),
                        conversation_id: "room".into(),
                        sender: sender.into(),
                        agent_id: agent.map(Into::into),
                        body: "Recorded message".into(),
                        created_at_ms: 0,
                    },
                    2,
                )
                .await
                .unwrap();
        }
        let room = room();
        room.route_operator_message(&store, "request", &[], false, 3)
            .await
            .unwrap();
        room.route_agent_update(&store, "result", 4).await.unwrap();
        room.route_agent_update(&store, "result", 5).await.unwrap();
        let lead = store
            .pending_conversation_deliveries("room", "lead")
            .await
            .unwrap();
        assert_eq!(lead.len(), 2);
        assert_eq!(lead[0].kind, "assignment");
        assert_eq!(lead[1].kind, "information");
        let reviewer = store
            .pending_conversation_deliveries("room", "reviewer")
            .await
            .unwrap();
        assert_eq!(reviewer.len(), 1);
        assert_eq!(reviewer[0].message_id, "result");
        assert!(
            room.route_operator_message(&store, "result", &[], true, 6)
                .await
                .is_err()
        );
        assert!(room.route_agent_update(&store, "request", 6).await.is_err());
    }

    #[test]
    fn command_and_information_paths_are_distinct() {
        let room = room();
        assert_eq!(
            room.operator_request(&[], false).unwrap()[0].agent_id,
            "lead"
        );
        assert_eq!(room.operator_request(&[], true).unwrap().len(), 3);
        assert!(room.delegate("lead", "coder").is_ok());
        assert!(room.delegate("coder", "lead").is_err());
        assert!(room.delegate("coder", "reviewer").is_err());
        let notices = room.inform("coder").unwrap();
        assert_eq!(notices.len(), 2);
        assert!(
            notices
                .iter()
                .all(|r| r.kind == RoomDeliveryKind::Information)
        );
        assert_eq!(notices[0].agent_id, "lead");
        assert_eq!(notices[0].reason, "report_to_supervisor");
        assert_eq!(notices[1].agent_id, "reviewer");
        assert!(
            room.inform("reviewer")
                .unwrap()
                .iter()
                .all(|r| r.agent_id != "coder")
        );
    }

    #[test]
    fn invalid_hierarchies_and_recipients_are_rejected() {
        let known = BTreeSet::from(["a".into(), "b".into()]);
        assert!(
            ConversationRoom::new(
                "a".into(),
                vec![member("a", Some("b"), &[]), member("b", Some("a"), &[]),],
                &known
            )
            .is_err()
        );
        assert!(
            ConversationRoom::new("a".into(), vec![member("a", None, &["missing"])], &known)
                .is_err()
        );
        assert!(room().operator_request(&["missing".into()], false).is_err());
        assert!(room().operator_request(&["coder".into()], true).is_err());
        assert!(room().inform("missing").is_err());
    }
}
