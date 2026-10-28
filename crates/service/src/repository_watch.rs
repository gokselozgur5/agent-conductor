//! Repository observation is data publication, never direct model-launch authority.
use crate::{Clock, PersistentControlService, trusted_project_registration};
use agent_conductor_domain::Task;
use agent_conductor_store::{
    EventLimits, ExecutionRecord, ExecutionState, RepositoryObservation, SemanticEvent, StoreError,
};
use conductor_runtime::{GitSnapshot, SnapshotEntry};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryWatch {
    pub id: String,
    pub project_id: String,
    pub source_agent_id: String,
    pub root: PathBuf,
}
#[derive(Clone, Debug, PartialEq, Eq)]
enum WorkspaceAttribution {
    Owned,
    External(String),
}
/// Per-watch monotonic debounce state; publication baseline remains in `SQLite`.
#[derive(Default)]
pub struct RepositoryWatchState {
    last: Option<String>,
    changed_at: std::time::Duration,
    pending_since: Option<std::time::Duration>,
    draining: bool,
}
/// Bounded round-robin cursor for active execution observation.
#[derive(Default)]
pub struct OwnedWatchState {
    cursor: String,
    observations: BTreeMap<String, RepositoryWatchState>,
    cycle: std::collections::BTreeSet<String>,
}
impl OwnedWatchState {
    fn observation(&mut self, token: &str) -> Result<&mut RepositoryWatchState, StoreError> {
        self.cycle.insert(token.into());
        if self.cycle.len() > 256 {
            return Err(invalid("owned watcher active execution capacity exceeded"));
        }
        if !self.observations.contains_key(token) && self.observations.len() == 256 {
            let retired = self
                .observations
                .keys()
                .find(|key| !self.cycle.contains(*key))
                .cloned()
                .ok_or_else(|| invalid("owned watcher cache capacity exceeded"))?;
            self.observations.remove(&retired);
        }
        Ok(self
            .observations
            .entry(token.into())
            .or_insert_with(|| RepositoryWatchState {
                // Baseline was already recorded before release: first observed writes
                // must debounce too, unlike an external repository's initial baseline.
                last: Some(String::new()),
                ..RepositoryWatchState::default()
            }))
    }
    fn finish_page(&mut self, next: String) {
        if next.is_empty() {
            self.observations
                .retain(|token, _| self.cycle.contains(token));
            self.cycle.clear();
        }
        self.cursor = next;
    }
}
impl RepositoryWatchState {
    /// Continue observing until debounce or bounded publication draining settles.
    #[must_use]
    pub fn needs_settle(&self) -> bool {
        self.pending_since.is_some() || self.draining
    }

    fn ready(&mut self, fingerprint: String, now: std::time::Duration) -> bool {
        let initial = self.last.is_none();
        if self.last.as_ref() != Some(&fingerprint) {
            self.last = Some(fingerprint);
            self.changed_at = now;
            self.pending_since.get_or_insert(now);
        }
        initial
            || self.draining
            || self.pending_since.is_some_and(|first| {
                now.saturating_sub(self.changed_at) >= std::time::Duration::from_millis(750)
                    || now.saturating_sub(first) >= std::time::Duration::from_secs(5)
            })
    }
    fn published(&mut self, count: usize) {
        self.pending_since = None;
        self.draining = count == 256;
    }
}

impl<C: Clock> PersistentControlService<C> {
    /// Records the trusted host's configured observation scope before dispatch starts.
    pub fn configure_repository_observation(&self, configs: &[RepositoryWatch]) {
        if let Ok(mut projects) = self.observed_projects.write() {
            projects.clear();
            for config in configs {
                projects
                    .entry(config.project_id.clone())
                    .and_modify(|old| {
                        if config.source_agent_id < *old {
                            old.clone_from(&config.source_agent_id);
                        }
                    })
                    .or_insert_with(|| config.source_agent_id.clone());
            }
        }
    }

    /// Flush the accepted fenced capture while the caller owns the lifecycle lock.
    pub(super) async fn publish_final_owned_capture(
        &self,
        token: &str,
        packet: &conductor_runtime::ResumePacket,
        manifest: &conductor_runtime::SnapshotManifest,
    ) -> Result<(), StoreError> {
        let task = self
            .store
            .task(&packet.contract.task_id)
            .await?
            .ok_or_else(|| invalid("final observation task absent"))?;
        let enabled = self
            .observed_projects
            .read()
            .map_err(|_| invalid("observation scope unavailable"))?
            .contains_key(&task.project_id);
        if !enabled {
            return Ok(());
        }
        let mut current = manifest.clone();
        current.entries.retain(|entry| entry.sha256.is_some());
        // Two bounded inventories can differ in at most 20,000 paths. Each
        // transaction advances at most 256; unchanged captured bytes are reused.
        for _ in 0..80 {
            self.validate_runtime_owner(packet).await?;
            if self
                .publish_owned_scan(token, packet, current.clone(), false)
                .await?
                < 256
            {
                return Ok(());
            }
        }
        Err(invalid("final observation exceeded bounded inventory"))
    }

    /// Observes a configured repository and publishes one bounded batch atomically.
    /// Initial observation establishes a baseline; subsequent calls drain at most
    /// 256 net changes. Other changes remain different from the persisted baseline.
    /// # Errors
    /// Refuses invalid registration, unsupported inventory and storage conflicts.
    pub async fn poll_repository(
        &self,
        config: &RepositoryWatch,
        state: &mut RepositoryWatchState,
        elapsed: std::time::Duration,
    ) -> Result<usize, StoreError> {
        if [&config.id, &config.project_id, &config.source_agent_id]
            .iter()
            .any(|s| s.is_empty() || s.len() > 128)
            || !config.root.is_absolute()
        {
            return Err(invalid("invalid repository watch configuration"));
        }
        let root = config
            .root
            .canonicalize()
            .map_err(|_| invalid("watch root unavailable"))?;
        if root != config.root {
            return Err(invalid("watch root must be canonical"));
        }
        let identity = trusted_project_registration(
            config.project_id.clone(),
            root.to_str().ok_or_else(|| invalid("watch path invalid"))?,
            0,
        )
        .map_err(|_| invalid("watch project identity invalid"))?;
        if self
            .store
            .project_for_identity(
                &identity.filesystem_identity,
                identity.git_common_dir_identity.as_deref(),
            )
            .await?
            .as_deref()
            != Some(config.project_id.as_str())
            || !self.store.agent_exists(&config.source_agent_id).await?
        {
            return Err(invalid(
                "watch requires registered project and source agent",
            ));
        }
        let observed = match conductor_runtime::observe_git(&root).await {
            Ok(observed) => observed,
            Err(conductor_runtime::SnapshotError::Changed) => return Ok(0),
            Err(_) => return Err(invalid("repository observation refused")),
        };
        let fingerprint = format!(
            "{}:{}:{}",
            observed.starting_head,
            observed.status_sha256,
            observed
                .manifest
                .sha256()
                .map_err(|_| invalid("watch digest failed"))?
        );
        if !state.ready(fingerprint, elapsed) {
            return Ok(0);
        }
        let count = self.publish_repository_scan(config, observed).await?;
        state.published(count);
        Ok(count)
    }

    /// Observes live private attempt trees for a configured project. Ownership is
    /// revalidated after filesystem reads and serialized with lifecycle mutations.
    /// # Errors
    /// Refuses invalid retained contracts, unsafe trees and publication conflicts.
    pub async fn poll_owned_workspaces(
        &self,
        project_id: &str,
        state: &mut OwnedWatchState,
        elapsed: std::time::Duration,
    ) -> Result<usize, StoreError> {
        let mut count = 0;
        let records = self
            .store
            .observable_execution_page(project_id, &state.cursor)
            .await?;
        let next = if records.len() == 16 {
            records
                .last()
                .map(|r| r.preparation.effect_token.clone())
                .unwrap_or_default()
        } else {
            String::new()
        };
        for record in records {
            let packet =
                conductor_runtime::ResumePacket::decode(record.preparation.packet_json.as_bytes())
                    .map_err(|_| invalid("invalid owned observation packet"))?;
            if packet.contract.adapter.kind == conductor_runtime::AdapterKind::Fake {
                continue;
            }
            let task = self
                .store
                .task(&packet.contract.task_id)
                .await?
                .ok_or_else(|| invalid("owned observation task absent"))?;
            if task.project_id != project_id {
                continue;
            }
            let Some(attribution) = self.workspace_attribution(&packet, &record).await? else {
                continue;
            };
            let root = packet.contract.workspace.clone();
            let observed =
                tokio::task::spawn_blocking(move || conductor_runtime::observe_attempt_tree(&root))
                    .await
                    .map_err(|_| invalid("owned observation interrupted"))?;
            let manifest = match observed {
                Ok(value) => value,
                Err(conductor_runtime::SnapshotError::Changed) => continue,
                Err(_) => return Err(invalid("owned observation refused")),
            };
            let fingerprint = manifest
                .sha256()
                .map_err(|_| invalid("owned observation digest failed"))?;
            if !state
                .observation(&record.preparation.effect_token)?
                .ready(fingerprint, elapsed)
            {
                continue;
            }
            let _guard = self.mutations.lock().await;
            if self.workspace_attribution(&packet, &record).await?.as_ref() != Some(&attribution) {
                continue;
            }
            let current = self
                .store
                .execution(&record.preparation.effect_token)
                .await?;
            if current.as_ref() != Some(&record) {
                continue;
            }
            let published = self
                .publish_workspace_scan(
                    &record.preparation.effect_token,
                    &packet,
                    manifest,
                    false,
                    &attribution,
                )
                .await?;
            state
                .observation(&record.preparation.effect_token)?
                .published(published);
            count += published;
        }
        state.finish_page(next);
        Ok(count)
    }

    async fn workspace_attribution(
        &self,
        packet: &conductor_runtime::ResumePacket,
        record: &ExecutionRecord,
    ) -> Result<Option<WorkspaceAttribution>, StoreError> {
        if packet
            .sha256()
            .map_err(|_| invalid("workspace packet hash"))?
            != record.preparation.packet_sha256
            || packet.contract.task_id != record.preparation.task_id
            || packet.contract.attempt_id != record.preparation.attempt_id
            || i64::try_from(packet.contract.lease_epoch).ok()
                != Some(record.preparation.lease_epoch)
        {
            return Err(invalid("workspace execution identity mismatch"));
        }
        let row = self
            .store
            .task(&packet.contract.task_id)
            .await?
            .ok_or_else(|| invalid("workspace task absent"))?;
        let task: Task = serde_json::from_str(&row.aggregate_json)
            .map_err(|_| invalid("workspace task invalid"))?;
        if task.state.is_terminal()
            || !task.attempt.as_ref().is_some_and(|attempt| {
                attempt.id.0 == packet.contract.attempt_id
                    && attempt.worker.0 == packet.contract.worker_id
                    && attempt.lease.epoch == packet.contract.lease_epoch
            })
        {
            return Ok(None);
        }
        if matches!(
            record.state,
            ExecutionState::Started | ExecutionState::Exited
        ) && self.validate_runtime_owner(packet).await.is_ok()
        {
            return Ok(Some(WorkspaceAttribution::Owned));
        }
        let observer = self
            .observed_projects
            .read()
            .map_err(|_| invalid("observation scope unavailable"))?
            .get(&row.project_id)
            .cloned();
        let Some(observer) = observer else {
            return Ok(None);
        };
        if self
            .store
            .agent(&observer)
            .await?
            .is_none_or(|agent| agent.access_mode != "read_only")
        {
            return Err(invalid(
                "external workspace observation requires a registered read-only observer",
            ));
        }
        Ok(Some(WorkspaceAttribution::External(observer)))
    }

    pub(super) async fn publish_owned_scan(
        &self,
        token: &str,
        packet: &conductor_runtime::ResumePacket,
        manifest: conductor_runtime::SnapshotManifest,
        initialize: bool,
    ) -> Result<usize, StoreError> {
        self.publish_workspace_scan(
            token,
            packet,
            manifest,
            initialize,
            &WorkspaceAttribution::Owned,
        )
        .await
    }

    async fn publish_workspace_scan(
        &self,
        token: &str,
        packet: &conductor_runtime::ResumePacket,
        manifest: conductor_runtime::SnapshotManifest,
        initialize: bool,
        attribution: &WorkspaceAttribution,
    ) -> Result<usize, StoreError> {
        let task = self
            .store
            .task(&packet.contract.task_id)
            .await?
            .ok_or_else(|| invalid("owned observation task absent"))?;
        let config = RepositoryWatch {
            id: format!("owned:{:x}", Sha256::digest(token.as_bytes())),
            project_id: task.project_id,
            source_agent_id: match attribution {
                WorkspaceAttribution::Owned => packet.contract.worker_id.clone(),
                WorkspaceAttribution::External(observer) => observer.clone(),
            },
            root: packet.contract.workspace.clone(),
        };
        let config_json = serde_json::to_string(&(
            token,
            packet.sha256().map_err(|_| invalid("owned packet hash"))?,
        ))
        .map_err(|_| invalid("owned configuration encoding"))?;
        let current = GitSnapshot {
            starting_head: packet.contract.starting_head.clone(),
            status_sha256: manifest
                .sha256()
                .map_err(|_| invalid("owned manifest hash"))?,
            manifest,
        };
        let stored = self
            .store
            .repository_observation(&config.id)
            .await
            .map_err(|_| invalid("owned baseline read"))?;
        let now = self.clock.read(self.boot_generation).wall_unix_ms;
        let (version, next, mut events) = match stored {
            None if initialize => (None, current, Vec::new()),
            Some((prior, version, bytes)) if prior == config_json && !initialize => {
                let old =
                    serde_json::from_str(&bytes).map_err(|_| invalid("owned baseline decoding"))?;
                let (next, events) = changes(&config, version, &old, &current, now)?;
                if next == old {
                    return Ok(0);
                }
                (Some(version), next, events)
            }
            _ => return Err(invalid("owned baseline identity mismatch or missing")),
        };
        let trigger = self
            .store
            .workflow_trigger(&packet.contract.task_id)
            .await?
            .map(|text| serde_json::from_str::<serde_json::Value>(&text))
            .transpose()
            .map_err(|_| invalid("invalid workflow trigger"))?;
        for event in &mut events {
            attribute_workspace_event(event, packet, token, attribution, trigger.as_ref())?;
        }
        let snapshot_json =
            serde_json::to_string(&next).map_err(|_| invalid("owned snapshot encoding"))?;
        self.store
            .publish_repository_observation(
                &RepositoryObservation {
                    id: &config.id,
                    config_json: &config_json,
                    expected_version: version,
                    snapshot_json: &snapshot_json,
                    now_ms: now,
                },
                &events,
                &EventLimits::default(),
            )
            .await
            .map_err(|_| invalid("owned publication refused"))?;
        Ok(events.len())
    }

    async fn publish_repository_scan(
        &self,
        config: &RepositoryWatch,
        mut current: GitSnapshot,
    ) -> Result<usize, StoreError> {
        let _guard = self.mutations.lock().await;
        // Absence is represented by absence in hook baselines, not retained Git tombstones.
        current.manifest.entries.retain(|e| e.sha256.is_some());
        let config_json =
            serde_json::to_string(config).map_err(|_| invalid("watch encoding failed"))?;
        let stored = self
            .store
            .repository_observation(&config.id)
            .await
            .map_err(|_| invalid("watch baseline read failed"))?;
        let now = self.clock.read(self.boot_generation).wall_unix_ms;
        let (version, next, events) = if let Some((prior_config, version, bytes)) = stored {
            if prior_config != config_json {
                return Err(invalid("watch configuration changed"));
            }
            let old: GitSnapshot =
                serde_json::from_str(&bytes).map_err(|_| invalid("invalid watch baseline"))?;
            let (next, events) = changes(config, version, &old, &current, now)?;
            if old == next {
                return Ok(0);
            }
            (Some(version), next, events)
        } else {
            (None, current, Vec::new())
        };
        let snapshot_json =
            serde_json::to_string(&next).map_err(|_| invalid("watch snapshot encoding failed"))?;
        self.store
            .publish_repository_observation(
                &RepositoryObservation {
                    id: &config.id,
                    config_json: &config_json,
                    expected_version: version,
                    snapshot_json: &snapshot_json,
                    now_ms: now,
                },
                &events,
                &EventLimits::default(),
            )
            .await
            .map_err(|_| invalid("watch publication failed"))?;
        Ok(events.len())
    }
}
fn attribute_workspace_event(
    event: &mut SemanticEvent,
    packet: &conductor_runtime::ResumePacket,
    token: &str,
    attribution: &WorkspaceAttribution,
    trigger: Option<&serde_json::Value>,
) -> Result<(), StoreError> {
    let mut payload: serde_json::Value =
        serde_json::from_str(&event.payload_json).map_err(|_| invalid("owned payload"))?;
    event.task_id = Some(packet.contract.task_id.clone());
    match attribution {
        WorkspaceAttribution::Owned => {
            event
                .source_session_id
                .clone_from(&packet.contract.attempt_id);
            event.correlation_id = token.into();
            inherit_trigger(event, trigger)?;
            event.attempt_id = Some(packet.contract.attempt_id.clone());
            event.lease_epoch = Some(packet.contract.lease_epoch);
            payload["attribution"] = "owned_workspace".into();
        }
        WorkspaceAttribution::External(_) => {
            event.topic = format!(
                "workspace.external.{}",
                event
                    .topic
                    .strip_prefix("file.")
                    .ok_or_else(|| invalid("unexpected workspace event"))?
            );
            event.correlation_id.clone_from(&event.id);
            payload["attribution"] = "external_workspace".into();
            payload["observed_attempt_id"] = packet.contract.attempt_id.clone().into();
        }
    }
    event.payload_json = payload.to_string();
    Ok(())
}

fn inherit_trigger(
    event: &mut SemanticEvent,
    trigger: Option<&serde_json::Value>,
) -> Result<(), StoreError> {
    if let Some(trigger) = trigger {
        event.correlation_id = trigger["correlation_id"]
            .as_str()
            .ok_or_else(|| invalid("trigger correlation absent"))?
            .into();
        event.causation_id = Some(
            trigger["id"]
                .as_str()
                .ok_or_else(|| invalid("trigger identity absent"))?
                .into(),
        );
        event.generation = u32::try_from(
            trigger["generation"]
                .as_u64()
                .ok_or_else(|| invalid("trigger generation absent"))?,
        )
        .map_err(|_| invalid("trigger generation overflow"))?
        .checked_add(1)
        .ok_or_else(|| invalid("trigger generation overflow"))?;
    }
    Ok(())
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}
fn changes(
    config: &RepositoryWatch,
    version: i64,
    old: &GitSnapshot,
    current: &GitSnapshot,
    now: i64,
) -> Result<(GitSnapshot, Vec<SemanticEvent>), StoreError> {
    let mut remaining: BTreeMap<String, SnapshotEntry> = old
        .manifest
        .entries
        .iter()
        .cloned()
        .map(|e| (e.path.clone(), e))
        .collect();
    let target: BTreeMap<String, SnapshotEntry> = current
        .manifest
        .entries
        .iter()
        .cloned()
        .map(|e| (e.path.clone(), e))
        .collect();
    let paths: std::collections::BTreeSet<_> =
        remaining.keys().chain(target.keys()).cloned().collect();
    let mut events = Vec::new();
    let mut next = old.clone();
    if old.starting_head != current.starting_head {
        events.push(envelope(config,version,"git.head_changed",None,&serde_json::json!({"before":old.starting_head,"after":current.starting_head,"attribution":"external"}),now)?);
        next.starting_head.clone_from(&current.starting_head);
    }
    for path in paths {
        if events.len() >= 256 {
            break;
        }
        let before = remaining.get(&path);
        let after = target.get(&path);
        if before == after {
            continue;
        }
        let topic = match (before, after) {
            (None, Some(_)) => "file.created",
            (Some(_), None) => "file.deleted",
            _ => "file.updated",
        };
        events.push(envelope(
            config,
            version,
            topic,
            Some(path.clone()),
            &serde_json::json!({"before":before,"after":after,"attribution":"external"}),
            now,
        )?);
        if let Some(after) = after {
            remaining.insert(path, after.clone());
        } else {
            remaining.remove(&path);
        }
    }
    let drained = remaining == target;
    next.manifest.entries = remaining.into_values().collect();
    if drained {
        next.status_sha256.clone_from(&current.status_sha256);
    }
    Ok((next, events))
}
fn envelope(
    config: &RepositoryWatch,
    version: i64,
    topic: &str,
    path: Option<String>,
    payload: &serde_json::Value,
    now: i64,
) -> Result<SemanticEvent, StoreError> {
    let identity = serde_json::to_vec(&(&config.id, version, topic, &path))
        .map_err(|_| invalid("event identity encoding failed"))?;
    let id = format!("hook:{:x}", Sha256::digest(identity));
    Ok(SemanticEvent {
        schema_version: 1,
        id: id.clone(),
        idempotency_key: id,
        source_agent_id: config.source_agent_id.clone(),
        source_session_id: format!("watch:{}", config.id),
        correlation_id: format!("watch:{}:{version}", config.id),
        causation_id: None,
        generation: 0,
        topic: topic.into(),
        project_id: Some(config.project_id.clone()),
        task_id: None,
        attempt_id: None,
        lease_epoch: None,
        path_scope: path,
        payload_json: serde_json::to_string(payload)
            .map_err(|_| invalid("event encoding failed"))?,
        created_at_ms: now,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owned_debounce_waits_for_first_burst_and_bounds_retained_state() {
        use std::time::Duration;
        let mut state = OwnedWatchState::default();
        assert!(
            !state
                .observation("worker")
                .unwrap()
                .ready("first".into(), Duration::ZERO)
        );
        assert!(
            !state
                .observation("worker")
                .unwrap()
                .ready("second".into(), Duration::from_millis(100))
        );
        assert!(
            state
                .observation("worker")
                .unwrap()
                .ready("second".into(), Duration::from_millis(850))
        );
        state.finish_page(String::new());
        assert_eq!(state.observations.len(), 1);
        state.finish_page(String::new());
        assert!(
            state.observations.is_empty(),
            "finished executions must not accumulate"
        );
        for i in 0..256 {
            state.observation(&format!("old-{i}")).unwrap();
        }
        assert!(state.observation("overflow").is_err());
        state.finish_page(String::new());
        for i in 0..256 {
            state.observation(&format!("new-{i}")).unwrap();
        }
        assert_eq!(state.observations.len(), 256);
        state.finish_page(String::new());
        assert!(state.observations.keys().all(|key| key.starts_with("new-")));
    }

    #[test]
    fn debounce_coalesces_bursts_but_bounds_continuous_change_and_drains_batches() {
        use std::time::Duration;
        let mut state = RepositoryWatchState::default();
        assert!(state.ready("baseline".into(), Duration::ZERO));
        state.published(0);
        assert!(!state.needs_settle());
        assert!(!state.ready("edit".into(), Duration::from_millis(100)));
        assert!(state.needs_settle());
        assert!(!state.ready("more".into(), Duration::from_millis(600)));
        assert!(!state.ready("more".into(), Duration::from_millis(1000)));
        assert!(state.ready("more".into(), Duration::from_millis(1350)));
        state.published(256);
        assert!(state.needs_settle());
        assert!(state.ready("more".into(), Duration::from_millis(1400)));
        state.published(1);
        assert!(!state.needs_settle());
        assert!(!state.ready("more".into(), Duration::from_millis(1500)));
        for i in 2..7 {
            assert!(!state.ready(format!("edit-{i}"), Duration::from_secs(i)));
        }
        assert!(state.ready("edit-7".into(), Duration::from_secs(7)));
    }
    fn snapshot(count: usize) -> GitSnapshot {
        GitSnapshot {
            starting_head: "a".repeat(40),
            status_sha256: "b".repeat(64),
            manifest: conductor_runtime::SnapshotManifest {
                version: 1,
                entries: (0..count)
                    .map(|i| SnapshotEntry {
                        path: format!("src/{i:04}"),
                        sha256: Some("c".repeat(64)),
                        bytes: 1,
                        executable: false,
                    })
                    .collect(),
            },
        }
    }
    #[test]
    fn bounded_batches_drain_storm_without_losing_paths_or_reusing_ids() {
        let config = RepositoryWatch {
            id: "watch".into(),
            project_id: "project".into(),
            source_agent_id: "source".into(),
            root: "/fixture".into(),
        };
        let mut baseline = snapshot(0);
        let target = snapshot(600);
        let mut ids = std::collections::BTreeSet::new();
        for (version, expected) in [(0, 256), (1, 256), (2, 88)] {
            let (next, events) = changes(&config, version, &baseline, &target, 100).unwrap();
            assert_eq!(events.len(), expected);
            for event in events {
                assert!(ids.insert(event.id));
                assert_eq!(event.topic, "file.created");
            }
            baseline = next;
        }
        assert_eq!(baseline, target);
        assert!(
            changes(&config, 3, &baseline, &target, 100)
                .unwrap()
                .1
                .is_empty()
        );
        let (partial, deleted) = changes(&config, 3, &baseline, &snapshot(0), 100).unwrap();
        assert_eq!(deleted.len(), 256);
        assert_eq!(partial.manifest.entries.len(), 344);
        assert!(
            deleted
                .iter()
                .all(|e| e.topic == "file.deleted" && !ids.contains(&e.id))
        );
    }
}
