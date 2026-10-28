use crate::{Store, StoreError};

#[derive(Debug, Clone, Copy)]
pub enum InventoryKind {
    Projects,
    Agents,
    Events,
    Subscriptions,
    Deliveries,
    Usage,
}
#[derive(Debug, sqlx::FromRow)]
pub struct InventoryRow {
    pub id: String,
    pub metadata: String,
}
impl Store {
    /// Read at most 101 metadata rows, including one pagination sentinel.
    /// Payloads, consumer state and claim tokens are deliberately excluded.
    /// # Errors
    /// Returns database read errors.
    pub async fn inventory_page(
        &self,
        kind: InventoryKind,
        after: Option<&str>,
    ) -> Result<Vec<InventoryRow>, StoreError> {
        let (table, fields) = match kind {
            InventoryKind::Usage => (
                "(SELECT id,task_id,payload_json,occurred_at_ms FROM audit_events WHERE kind='vendor.usage_reported')",
                "'id',id,'task_id',task_id,'observed_at_ms',occurred_at_ms,'invocation_id',json_extract(payload_json,'$.invocation_id'),'attempt_id',json_extract(payload_json,'$.attempt_id'),'lease_epoch',json_extract(payload_json,'$.lease_epoch'),'role',json_extract(payload_json,'$.role'),'agent_id',json_extract(payload_json,'$.agent_id'),'adapter',json_extract(payload_json,'$.adapter'),'packet_sha256',json_extract(payload_json,'$.packet_sha256'),'output_sha256',json_extract(payload_json,'$.output_sha256'),'report',json_extract(payload_json,'$.report')",
            ),
            InventoryKind::Projects => (
                "projects",
                "'id',id,'canonical_path',canonical_path,'created_at_ms',created_at_ms",
            ),
            InventoryKind::Agents => (
                "agents",
                "'id',id,'adapter_kind',adapter_kind,'access_mode',access_mode,'protocol_fingerprint',protocol_fingerprint,'capability_hash',capability_hash,'created_at_ms',created_at_ms",
            ),
            InventoryKind::Events => (
                "semantic_events",
                "'id',id,'source_agent_id',source_agent_id,'correlation_id',correlation_id,'causation_id',causation_id,'generation',generation,'topic',topic,'project_id',project_id,'task_id',task_id,'attempt_id',attempt_id,'lease_epoch',lease_epoch,'path_scope',path_scope,'created_at_ms',created_at_ms,'expires_at_ms',expires_at_ms",
            ),
            InventoryKind::Subscriptions => (
                "event_subscriptions",
                "'id',id,'agent_id',agent_id,'topic_filter',topic_filter,'project_id',project_id,'path_prefix',path_prefix,'echo_source',echo_source,'active',active",
            ),
            InventoryKind::Deliveries => (
                "event_deliveries",
                "'id',id,'event_id',event_id,'subscription_id',subscription_id,'target_agent_id',target_agent_id,'status',status,'attempts',attempts,'available_at_ms',available_at_ms,'lease_expires_at_ms',lease_expires_at_ms,'acked_at_ms',acked_at_ms,'stale_reason',stale_reason",
            ),
        };
        // Only static identifiers above enter SQL; the caller cursor is bound.
        Ok(sqlx::query_as(&format!("SELECT id,json_object({fields}) AS metadata FROM {table} WHERE id > ? ORDER BY id LIMIT 101"))
            .bind(after.unwrap_or("")).fetch_all(&self.inner.readers).await?)
    }
}
