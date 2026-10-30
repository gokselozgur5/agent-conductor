use crate::{Clock, ConversationRoom, PersistentControlService, RoomConfiguration};
use agent_conductor_store::StoreError;

impl<C: Clock> PersistentControlService<C> {
    fn validate_room(&self, config: RoomConfiguration) -> Result<ConversationRoom, StoreError> {
        let agents = self
            .conversation_agents
            .read()
            .map_err(|_| StoreError::Integrity("conversation configuration unavailable".into()))?;
        ConversationRoom::new(
            config.coordinator,
            config.members,
            &agents.keys().cloned().collect(),
        )
    }

    /// Configures a shared room using operator-owned command relationships.
    /// # Errors
    /// Rejects invalid hierarchy, unknown agents or changed durable authority.
    pub async fn configure_room(
        &self,
        conversation_id: &str,
        config: RoomConfiguration,
    ) -> Result<(), StoreError> {
        self.validate_room(config.clone())?;
        let encoded = serde_json::to_string(&config)
            .map_err(|_| StoreError::Integrity("room encoding failed".into()))?;
        self.store
            .configure_conversation_room(conversation_id, &encoded)
            .await
    }

    async fn load_room(&self, conversation_id: &str) -> Result<ConversationRoom, StoreError> {
        let encoded = self
            .store
            .conversation_room(conversation_id)
            .await?
            .ok_or_else(|| StoreError::Integrity("conversation room is not configured".into()))?;
        let config = serde_json::from_str(&encoded)
            .map_err(|_| StoreError::Integrity("stored room configuration invalid".into()))?;
        self.validate_room(config)
    }

    /// Routes an operator message once; the durable dispatcher admits recipients.
    /// # Errors
    /// Rejects missing rooms, unknown recipients and conflicting routing replay.
    pub async fn route_room_request(
        &self,
        message_id: &str,
        recipients: &[String],
        everyone: bool,
    ) -> Result<(), StoreError> {
        let message = self
            .store
            .conversation_message(message_id)
            .await?
            .ok_or_else(|| StoreError::Integrity("room message missing".into()))?;
        self.load_room(&message.conversation_id)
            .await?
            .route_operator_message(
                &self.store,
                message_id,
                recipients,
                everyone,
                self.clock.read(self.boot_generation).wall_unix_ms,
            )
            .await
    }

    /// Reconciles persisted room work before native dispatch. A crash between
    /// routing and admission is recovered by the same idempotent queue binding.
    /// # Errors
    /// Returns configuration or persistence errors without inventing delivery.
    pub(crate) async fn reconcile_room_delivery(&self) -> Result<bool, StoreError> {
        if let Some((message, conversation)) = self.store.next_unrouted_room_reply().await? {
            self.load_room(&conversation)
                .await?
                .route_agent_update(
                    &self.store,
                    &message,
                    self.clock.read(self.boot_generation).wall_unix_ms,
                )
                .await?;
            return Ok(true);
        }
        if let Some((message, agent)) = self.store.next_room_assignment().await? {
            self.queue_conversation_turn(&message, &agent).await?;
            return Ok(true);
        }
        Ok(false)
    }
}
