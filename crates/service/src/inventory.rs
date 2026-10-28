use crate::{Clock, PersistentControlService, json_error, store_error};
use agent_conductor_store::InventoryKind;
use conductor_protocol::{ErrorBody, ErrorCode, InventoryResource, Response};
impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn inventory(
        &self,
        resource: InventoryResource,
        after: Option<String>,
    ) -> Result<Response, ErrorBody> {
        if after
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 4096 || value.contains('\0'))
        {
            return Err(ErrorBody::new(
                ErrorCode::Validation,
                "invalid inventory cursor",
            ));
        }
        let kind = match resource {
            InventoryResource::Projects => InventoryKind::Projects,
            InventoryResource::Agents => InventoryKind::Agents,
            InventoryResource::Events => InventoryKind::Events,
            InventoryResource::Subscriptions => InventoryKind::Subscriptions,
            InventoryResource::Deliveries => InventoryKind::Deliveries,
            InventoryResource::Usage => InventoryKind::Usage,
        };
        let mut rows = self
            .store
            .inventory_page(kind, after.as_deref())
            .await
            .map_err(store_error)?;
        let has_more = rows.len() > 100;
        rows.truncate(100);
        let next_after = if has_more {
            rows.last().map(|row| row.id.clone())
        } else {
            None
        };
        let entries = rows
            .into_iter()
            .map(|row| serde_json::from_str(&row.metadata).map_err(json_error))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::Inventory {
            resource,
            entries,
            next_after,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ControlService;
    use agent_conductor_store::{NewAgent, Store};
    use conductor_protocol::Request;

    #[tokio::test]
    async fn inventory_pages_preserve_ids_and_reject_invalid_cursors() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("state.sqlite3"))
            .await
            .unwrap();
        for number in 0..105 {
            store
                .create_agent(NewAgent {
                    id: &format!("agent-{number:03}"),
                    adapter_kind: "test",
                    protocol_fingerprint: "v1",
                    capability_hash: "hash",
                    access_mode: "read_only",
                    created_at_ms: 1,
                })
                .await
                .unwrap();
        }
        let service = PersistentControlService::start(store, 1).await.unwrap();
        let mut cursor = None;
        let mut ids = Vec::new();
        loop {
            let Response::Inventory {
                resource,
                entries,
                next_after,
            } = service
                .handle(Request::Inventory {
                    resource: InventoryResource::Agents,
                    after: cursor,
                })
                .await
                .unwrap()
            else {
                panic!("inventory expected")
            };
            assert_eq!(resource, InventoryResource::Agents);
            assert!(entries.len() <= 100);
            for row in entries {
                assert_eq!(row["access_mode"], "read_only");
                ids.push(row["id"].as_str().unwrap().to_owned());
            }
            if next_after.is_none() {
                break;
            }
            cursor = next_after;
        }
        assert_eq!(
            ids,
            (0..105)
                .map(|n| format!("agent-{n:03}"))
                .collect::<Vec<_>>()
        );
        for cursor in [String::new(), "bad\0cursor".into(), "x".repeat(4097)] {
            assert!(
                service
                    .handle(Request::Inventory {
                        resource: InventoryResource::Agents,
                        after: Some(cursor)
                    })
                    .await
                    .is_err()
            );
        }
        assert!(
            serde_json::from_str::<Request>(
                r#"{"type":"inventory","resource":"credentials","after":null}"#
            )
            .is_err()
        );
    }
    #[tokio::test]
    async fn inventory_exposes_causality_without_payload_or_claim_token() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("state.sqlite3"))
            .await
            .unwrap();
        for id in ["source", "consumer"] {
            store
                .create_agent(NewAgent {
                    id,
                    adapter_kind: "test",
                    protocol_fingerprint: "v1",
                    capability_hash: "hash",
                    access_mode: "read_only",
                    created_at_ms: 1,
                })
                .await
                .unwrap();
        }
        let service = PersistentControlService::start(store, 1).await.unwrap();
        for value in [
            serde_json::json!({"type":"subscribe_events","subscription":{"schema_version":1,"id":"sub","agent_id":"consumer","topic_filter":"file.*","project_id":null,"path_prefix":null,"echo_source":false}}),
            serde_json::json!({"type":"publish_event","event":{"schema_version":1,"id":"event","idempotency_key":"event","source_agent_id":"source","source_session_id":"session","correlation_id":"chain","causation_id":null,"generation":0,"topic":"file.created","project_id":null,"task_id":null,"attempt_id":null,"lease_epoch":null,"path_scope":"<img src=x>","payload":{"private":"OMIT_PAYLOAD"}}}),
        ] {
            service
                .handle(serde_json::from_value(value).unwrap())
                .await
                .unwrap();
        }
        let Response::EventDelivery {
            delivery: Some(delivery),
        } = service
            .handle(Request::ClaimEvent {
                agent_id: "consumer".into(),
            })
            .await
            .unwrap()
        else {
            panic!("delivery expected")
        };
        for resource in [
            InventoryResource::Events,
            InventoryResource::Subscriptions,
            InventoryResource::Deliveries,
        ] {
            let response = service
                .handle(Request::Inventory {
                    resource,
                    after: None,
                })
                .await
                .unwrap();
            let serialized = serde_json::to_string(&response).unwrap();
            assert!(!serialized.contains("OMIT_PAYLOAD"));
            assert!(!serialized.contains(&delivery.claim_token));
            assert!(!serialized.contains("claim_token"));
            let Response::Inventory {
                entries,
                next_after,
                ..
            } = response
            else {
                panic!("inventory expected")
            };
            assert_eq!(entries.len(), 1);
            assert_eq!(next_after, None);
            match resource {
                InventoryResource::Events => {
                    assert_eq!(entries[0]["correlation_id"], "chain");
                    assert_eq!(entries[0]["path_scope"], "<img src=x>");
                }
                InventoryResource::Deliveries => {
                    assert_eq!(entries[0]["status"], "leased");
                    assert_eq!(entries[0]["attempts"], 1);
                }
                InventoryResource::Subscriptions => {
                    assert_eq!(entries[0]["topic_filter"], "file.*");
                }
                _ => unreachable!(),
            }
        }
    }
}
