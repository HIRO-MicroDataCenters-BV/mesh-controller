use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use crate::client::kube_client::ClientError;
use crate::config::configuration::PeriodicSnapshotConfig;
use crate::config::configuration::TombstoneConfig;
use crate::kube::dynamic_object_ext::DynamicObjectExt;
use crate::kube::event::KubeEvent;
use crate::kube::subscriptions::Subscriptions;
use crate::kube::subscriptions::Version;
use crate::kube::types::NamespacedName;
use crate::merge::types::MergeResult;
use crate::merge::types::Tombstone;
use crate::mesh::event::MeshEvent;
use crate::mesh::operation_ext::OperationExt;
use crate::mesh::operation_log::OperationLog;
use crate::mesh::operation_log::Ready;
use crate::mesh::topic::MeshTopic;
use crate::network::discovery::nodes::Nodes;
use crate::network::discovery::nodes::PeerEvent;
use crate::network::discovery::types::Membership;
use crate::utils::types::Clock;
use anyhow::Context;
use anyhow::Result;
use futures::StreamExt;
use kube::api::DynamicObject;
use kube::api::GroupVersionKind;
use loole::RecvStream;
use p2panda_core::PublicKey;
use p2panda_core::{Operation, PrivateKey};
use p2panda_net::SystemEvent;
use p2panda_store::MemoryStore;
use p2panda_stream::operation::IngestError;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing::Span;
use tracing::span;
use tracing::{debug, error, trace, warn};

use super::operations::LinkedOperations;
use super::partition::Partition;
use super::topic::InstanceId;
use super::{operations::Extensions, topic::MeshLogId};

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_FORCED_SYNC_MAX_ATTEMPTS: usize = 10;

#[derive(Clone, Debug)]
pub struct UpdateLogIdResult {
    pub is_obsolete_operation: bool,
    pub is_new_log: bool,
    pub old_log: Option<MeshLogId>,
}

pub struct MeshActor {
    instance_id: InstanceId,
    network_tx: mpsc::Sender<Operation<Extensions>>,
    network_rx: mpsc::Receiver<Operation<Extensions>>,
    event_rx: RecvStream<KubeEvent>,
    system_events: broadcast::Receiver<SystemEvent<MeshTopic>>,
    subscriptions: Subscriptions,
    operation_log: OperationLog,
    operations: LinkedOperations,
    partition: Partition,
    clock: Arc<dyn Clock>,
    cancelation: CancellationToken,
    snapshot_config: PeriodicSnapshotConfig,
    tombstone_config: TombstoneConfig,
    last_snapshot_time: SystemTime,
    own_log_id: MeshLogId,
    membership: Membership,
    nodes: Nodes,
}

impl MeshActor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key: PrivateKey,
        snapshot_config: PeriodicSnapshotConfig,
        tombstone_config: TombstoneConfig,
        instance_id: InstanceId,
        partition: Partition,
        clock: Arc<dyn Clock>,
        cancelation: CancellationToken,
        nodes: Nodes,
        network_tx: mpsc::Sender<Operation<Extensions>>,
        network_rx: mpsc::Receiver<Operation<Extensions>>,
        event_rx: RecvStream<KubeEvent>,
        system_events: broadcast::Receiver<SystemEvent<MeshTopic>>,
        subscriptions: Subscriptions,
        store: MemoryStore<MeshLogId, Extensions>,
    ) -> MeshActor {
        let own_log_id = MeshLogId(instance_id.clone());
        let operations = LinkedOperations::new(key.clone(), instance_id.clone());
        let operation_log = OperationLog::new(own_log_id.clone(), key.public_key(), store.clone());

        let membership = nodes.get_membership(clock.now_millis());
        tracing::info!("initial membership {}", membership.to_string());
        MeshActor {
            last_snapshot_time: clock.now(),
            operations,
            operation_log,
            instance_id,
            network_tx,
            network_rx,
            event_rx,
            subscriptions,
            partition,
            clock,
            cancelation,
            snapshot_config,
            tombstone_config,
            own_log_id,
            membership,
            system_events,
            nodes,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut interval = tokio::time::interval(DEFAULT_TICK_INTERVAL);
        loop {
            tokio::select! {
                Some(event) = self.event_rx.next() => {
                    let span = span!(Level::DEBUG, "kube_event", id = event.get_id());
                    if let Err(err) = self.on_outgoing_to_network(&span, event).await {
                        error!(parent: &span, "error while processing kube event, {}", err);
                    }
                }
                Some(operation) = self.network_rx.recv() => {
                    let span = span!(Level::DEBUG, "incoming", op = %operation.get_id()?);
                    if let Err(err) = self.on_incoming_from_network(&span, operation).await {
                        error!(parent: &span, "error while processing network event, {}", err);
                    }
                }
                _ = interval.tick() => {
                    let span = span!(Level::DEBUG, "tick", ts = self.clock.now_millis().to_string());
                    if let Err(err) = self.on_tick(&span).await {
                        error!(parent: &span, "error on tick, {}", err);
                    }
                },
                Ok(event) = self.system_events.recv() => {
                    self.on_system_event(event).await;
                }
                _ = self.cancelation.cancelled() => break,
            }
        }
        Ok(())
    }

    async fn on_system_event(&mut self, event: SystemEvent<MeshTopic>) {
        match event {
            SystemEvent::GossipNeighborDown { peer, .. } => self.on_peer_down(peer).await,
            SystemEvent::GossipNeighborUp { peer, .. } => self.on_peer_up(peer).await,
            SystemEvent::PeerDiscovered { peer } => self.on_peer_discovered(peer).await,
            SystemEvent::SyncStarted { peer, .. } => {
                tracing::trace!("sync started: {}", peer.to_hex())
            }
            SystemEvent::SyncDone { peer, .. } => tracing::trace!("sync done: {}", peer.to_hex()),
            SystemEvent::SyncFailed { peer, .. } => {
                tracing::trace!("sync failed: {}", peer.to_hex())
            }
            _ => {}
        }
    }

    async fn on_peer_discovered(&mut self, peer: p2panda_core::PublicKey) {
        let now = self.clock.now_millis();
        let span = span!(Level::DEBUG, "peer_discovered", id = now.to_string());
        let maybe_updated_membership = self
            .nodes
            .on_event(&span, PeerEvent::PeerDiscovered { peer, now });
        self.update_membership(&span, maybe_updated_membership)
            .await;
    }

    async fn on_peer_up(&mut self, peer: p2panda_core::PublicKey) {
        let now = self.clock.now_millis();
        let span = span!(Level::DEBUG, "peer_up", id = now.to_string());
        tracing::debug!(parent: &span, "system event received");
        let maybe_updated_membership = self.nodes.on_event(&span, PeerEvent::PeerUp { peer, now });
        self.update_membership(&span, maybe_updated_membership)
            .await;
    }

    async fn on_peer_down(&mut self, peer: p2panda_core::PublicKey) {
        let now = self.clock.now_millis();
        let span = span!(Level::DEBUG, "peer_down", id = now.to_string());
        tracing::debug!(parent: &span, "system event received");
        let maybe_updated_membership = self
            .nodes
            .on_event(&span, PeerEvent::PeerDown { peer, now });
        self.update_membership(&span, maybe_updated_membership)
            .await;
    }

    async fn update_membership(
        &mut self,
        span: &Span,
        maybe_updated_membership: Option<Membership>,
    ) {
        if let Some(membership) = maybe_updated_membership {
            if !self.membership.is_equal(&membership) {
                if let Err(err) = self.on_membership_change(span, membership).await {
                    error!(parent: span, "membership change error {err:?}");
                }
            }
        }
    }

    async fn on_membership_change(&mut self, span: &Span, membership: Membership) -> Result<()> {
        self.membership = membership;
        debug!(parent: span, "membership update: {}", self.membership.to_string());
        let merge_results = self.partition.mesh_onchange_membership(
            span,
            &self.membership,
            &self.instance_id.zone,
        )?;
        self.on_merge_results(span, merge_results).await;
        self.on_ready(span).await?;
        Ok(())
    }

    async fn on_outgoing_to_network(&mut self, span: &Span, event: KubeEvent) -> Result<()> {
        self.on_kube_event(span, event).await?;
        self.on_ready(span).await?;
        Ok(())
    }

    async fn on_kube_event(&mut self, span: &Span, event: KubeEvent) -> Result<()> {
        let update_result = self
            .partition
            .kube_apply(span, event, &self.instance_id.zone)?;
        let event: Option<MeshEvent> = update_result.into();

        if let Some(event) = event {
            let operation = self.operations.next(event);
            // No need to check for new log because the produced operation is for own log which never changes in current instance,
            // thus should never trigger any membership updates
            self.operation_log.insert(span, operation).await?;
        }
        Ok(())
    }

    async fn on_ready(&mut self, span: &Span) -> Result<()> {
        let active_logs = self.nodes.get_remote_active_logs();
        if let Some(mut ready) = self.operation_log.get_ready(span, &active_logs).await {
            self.on_ready_outgoing(&mut ready).await;
            self.on_ready_incoming(&mut ready).await?;
        }
        Ok(())
    }

    async fn on_incoming_from_network(
        &mut self,
        span: &Span,
        operation: Operation<Extensions>,
    ) -> Result<()> {
        self.insert_operation(span, operation).await?;
        self.on_ready(span).await
    }

    async fn insert_operation(
        &mut self,
        span: &Span,
        operation: Operation<Extensions>,
    ) -> Result<()> {
        let peer = operation.header.public_key;

        let Operation { header, .. } = &operation;
        let Some(extensions) = header.extensions.as_ref() else {
            return Err(IngestError::MissingHeaderExtension("extension".into()).into());
        };

        let incoming_log_id = extensions.log_id.clone();

        let UpdateLogIdResult {
            is_obsolete_operation,
            is_new_log,
            old_log,
        } = self.update_log_id(span, &peer, &incoming_log_id);
        if is_new_log {
            self.operation_log
                .update_active_log(incoming_log_id.to_owned(), old_log);
        }
        if !is_obsolete_operation {
            self.operation_log.insert(span, operation).await?;
        } else {
            debug!(parent: span, "skipping operation, obsolete log.");
        }
        if is_new_log {
            debug!(parent: span, instance_id = ?incoming_log_id.0.to_string(), "new log detected");
            let membership = self.nodes.get_membership(self.clock.now_millis());
            self.update_membership(span, Some(membership)).await;
        }
        Ok(())
    }

    fn update_log_id(
        &mut self,
        span: &Span,
        incoming_source: &PublicKey,
        incoming_log_id: &MeshLogId,
    ) -> UpdateLogIdResult {
        match self.nodes.get_active_log(incoming_source) {
            Some(active) => match active.0.start_time.cmp(&incoming_log_id.0.start_time) {
                std::cmp::Ordering::Less => {
                    debug!(
                        parent: span,
                        "new log {} found from peer, and replaced old {}",
                        incoming_log_id.0.to_string(), active.0.to_string()
                    );
                    self.nodes.update_log(
                        span,
                        incoming_source.to_owned(),
                        incoming_log_id.to_owned(),
                        self.clock.now_millis(),
                    );
                    UpdateLogIdResult {
                        is_obsolete_operation: false,
                        is_new_log: true,
                        old_log: Some(active),
                    }
                }
                std::cmp::Ordering::Equal => UpdateLogIdResult {
                    is_obsolete_operation: false,
                    is_new_log: false,
                    old_log: None,
                },
                std::cmp::Ordering::Greater => UpdateLogIdResult {
                    is_obsolete_operation: true,
                    is_new_log: false,
                    old_log: None,
                },
            },
            None => {
                debug!(parent: span, "new log {} found from peer", incoming_log_id.0.to_string());
                self.nodes.update_log(
                    span,
                    incoming_source.to_owned(),
                    incoming_log_id.to_owned(),
                    self.clock.now_millis(),
                );
                UpdateLogIdResult {
                    is_obsolete_operation: false,
                    is_new_log: true,
                    old_log: None,
                }
            }
        }
    }

    async fn on_ready_incoming(&mut self, ready: &mut Ready) -> Result<(), anyhow::Error> {
        for (log_id, ops) in ready.take_incoming().into_iter() {
            for operation in ops.into_iter() {
                let span = span!(Level::DEBUG, "apply-operation", id = %operation.get_id()?);
                let pointer = operation.header.seq_num;
                if let Some(body) = operation.body {
                    let mesh_event = MeshEvent::try_from(body.to_bytes())?;
                    let merge_results = self.partition.mesh_apply(
                        &span,
                        mesh_event,
                        &log_id.0.zone,
                        &self.instance_id.zone,
                        &self.membership,
                    )?;
                    let event_types: Vec<&str> =
                        merge_results.iter().map(|e| e.event_type()).collect();
                    debug!(
                        parent: &span,
                        "merge result: {:?}",
                        event_types
                    );
                    self.on_merge_results(&span, merge_results).await;
                    self.operation_log.advance_log_pointer(&log_id, pointer + 1);
                } else {
                    error!(parent: span, "event has no body");
                }
            }
        }
        Ok(())
    }

    async fn on_merge_results(&mut self, span: &Span, merge_results: Vec<MergeResult>) {
        for merge_result in merge_results.into_iter() {
            let ok_or_error = self.on_merge_result(span, merge_result).await;
            if let Err(err) = ok_or_error {
                error!(parent: span, "error while merging {err}");
            }
        }
    }

    async fn on_merge_result(&mut self, span: &Span, merge_result: MergeResult) -> Result<()> {
        let event = if let MergeResult::Update { event, .. } = &merge_result {
            event.clone()
        } else {
            None
        };

        let initial_ok_or_error = self.kube_apply(span, merge_result).await;

        let final_ok_or_result = match initial_ok_or_error {
            Ok(PersistenceResult::Conflict {
                gvk,
                name,
                operation_type,
            }) => self.forced_sync(span, gvk, name, operation_type).await,
            Ok(ok) => Ok(ok),
            Err(err) => Err(err),
        };

        if let (Ok(PersistenceResult::Persisted(version)), Some(mut event)) =
            (&final_ok_or_result, event)
        {
            event.set_zone_version(*version);
            let operation = self.operations.next(event);
            debug!(parent: span, "inserting new event from persisted operation");
            // this insert into own log therefore no need to check for new logs and update membership
            self.operation_log.insert(span, operation).await?;
        }
        final_ok_or_result.map(|_| ())
    }

    async fn kube_apply(
        &mut self,
        span: &Span,
        merge_result: MergeResult,
    ) -> Result<PersistenceResult> {
        match merge_result {
            MergeResult::Create { object } => self.kube_patch_apply(span, object).await,
            MergeResult::Update { object, .. } => self.kube_patch_apply(span, object).await,
            MergeResult::Delete(Tombstone {
                gvk,
                name,
                resource_version,
                ..
            }) => self.kube_delete(span, &gvk, &name, resource_version).await,
            MergeResult::Skip | MergeResult::Tombstone { .. } => Ok(PersistenceResult::Skipped),
        }
    }

    async fn forced_sync(
        &mut self,
        span: &Span,
        gvk: GroupVersionKind,
        name: NamespacedName,
        operation_type: OperationType,
    ) -> Result<PersistenceResult> {
        let mut attempts = DEFAULT_FORCED_SYNC_MAX_ATTEMPTS;
        let mut persistence_result = PersistenceResult::Skipped;
        while attempts > 0 {
            if let Some(object) = self.subscriptions.client().get(&gvk, &name).await? {
                let version = object.get_resource_version();
                let name = object.get_namespaced_name();
                let event = match operation_type {
                    OperationType::Update => KubeEvent::Update { version, object },
                    OperationType::Delete => KubeEvent::Delete { version, object },
                };
                if let Err(err) = self.on_kube_event(span, event).await {
                    error!(parent: span, "on_event error during forced sync {err}");
                }
                self.partition.update_resource_version(&name, version);
                if let Some(current) = self.partition.get(&name) {
                    persistence_result = match operation_type {
                        OperationType::Update => self.kube_patch_apply(span, current).await?,
                        OperationType::Delete => {
                            self.kube_delete(span, &gvk, &name, current.get_resource_version())
                                .await?
                        }
                    };
                    let PersistenceResult::Conflict { .. } = persistence_result else {
                        return Ok(persistence_result);
                    };
                } else {
                    return Ok(PersistenceResult::Skipped);
                }
            } else {
                warn!(parent: span, "object {gvk:?} is no longer present");
                return Ok(PersistenceResult::Skipped);
            }
            attempts -= 1;
        }
        warn!(
            parent: span,
            "Conflicts: Number of attempts ({DEFAULT_FORCED_SYNC_MAX_ATTEMPTS}) is exhausted while updating object {}",
            name
        );
        Ok(persistence_result)
    }

    async fn kube_patch_apply(
        &mut self,
        span: &Span,
        mut object: DynamicObject,
    ) -> Result<PersistenceResult> {
        let gvk = object.get_gvk()?;
        let name = object.get_namespaced_name();

        let existing = self.subscriptions.client().get(&gvk, &name).await?;

        object.metadata.managed_fields = None;
        if let Some(existing) = existing {
            let existing_version = existing.get_resource_version();
            object.types = existing.types;
            object.metadata.uid = existing.metadata.uid;
            debug!(
                parent: span,
                "patch apply: existing version {}, object version {}",
                existing_version,
                object.get_resource_version()
            )
        } else {
            object.metadata.uid = None;
            object.metadata.resource_version = None;
        }

        let ok_or_error = self.subscriptions.client().patch_apply(object).await;

        match ok_or_error {
            Ok(new_version) => {
                self.partition.update_resource_version(&name, new_version);
                Ok(PersistenceResult::Persisted(new_version))
            }
            Err(ClientError::VersionConflict) => {
                debug!(parent: span, "Version Conflict for resource {}", name);
                Ok(PersistenceResult::Conflict {
                    gvk,
                    name,
                    operation_type: OperationType::Update,
                })
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn kube_delete(
        &mut self,
        span: &Span,
        gvk: &GroupVersionKind,
        name: &NamespacedName,
        version: Version,
    ) -> Result<PersistenceResult> {
        let existing = self.subscriptions.client().get(gvk, name).await?;

        if let Some(existing) = existing {
            let existing_version = existing.get_resource_version();

            if existing_version != version {
                return Ok(PersistenceResult::Conflict {
                    gvk: gvk.to_owned(),
                    name: name.to_owned(),
                    operation_type: OperationType::Delete,
                });
            }

            let object_or_status = self.subscriptions.client().delete(gvk, name).await?;
            object_or_status.map_right(|status|{
                if status.is_failure() {
                    error!(parent: span, %name, %status.code, %status.message, %status.reason, "delete object failure");
                }
            });
        } else {
            warn!(parent: span, %name, "Object not found. Skipping delete.");
        }
        let version = self.subscriptions.client().get_latest_version().await?;
        Ok(PersistenceResult::Persisted(version))
    }

    async fn on_ready_outgoing(&mut self, ready: &mut Ready) {
        for operation in ready.take_outgoing().into_iter() {
            let pointer = operation.header.seq_num;
            self.network_tx.send(operation).await.ok();
            self.operation_log
                .advance_log_pointer(&self.own_log_id, pointer + 1);
        }
    }

    async fn on_tick(&mut self, span: &Span) -> Result<()> {
        // Membership Check
        self.update_membership(
            span,
            self.nodes.on_event(
                span,
                PeerEvent::Tick {
                    now: self.clock.now_millis(),
                },
            ),
        )
        .await;

        // Send/Receive
        self.on_ready(span).await?;

        // Cleanup
        let truncate_size =
            self.operations.count_since_snapshot() >= self.snapshot_config.snapshot_max_log;
        let duration_since_last_snapshot = self
            .clock
            .now()
            .duration_since(self.last_snapshot_time)
            .context("compute duration since last snapshot time")?
            .as_secs();
        let snapshot_time =
            duration_since_last_snapshot > self.snapshot_config.snapshot_interval_seconds;
        if truncate_size || snapshot_time {
            self.send_snapshot(span).await?;
            let obsolete_log_ids = self.nodes.take_obsolete_log_ids();
            self.operation_log
                .truncate_obsolete_logs(span, obsolete_log_ids)
                .await?;
        }

        self.partition
            .drop_tombstones(self.tombstone_config.tombstone_retention_interval_seconds);

        Ok(())
    }

    async fn send_snapshot(&mut self, span: &Span) -> Result<()> {
        trace!(parent: span, "Periodic snapshot");
        let version = self.subscriptions.client().get_latest_version().await?;
        let event = self
            .partition
            .mesh_gen_snapshot(&self.instance_id.zone, version);
        let operation = self.operations.next(event);
        self.on_incoming_from_network(span, operation).await?;
        self.last_snapshot_time = self.clock.now();
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum OperationType {
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub enum PersistenceResult {
    Persisted(Version),
    Conflict {
        gvk: GroupVersionKind,
        name: NamespacedName,
        operation_type: OperationType,
    },
    Skipped,
}
