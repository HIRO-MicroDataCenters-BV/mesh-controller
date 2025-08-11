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
use crate::mesh::operation_log::OperationLog;
use crate::mesh::operation_log::Ready;
use crate::network::discovery::event::MembershipEvent;
use crate::network::discovery::types::Membership;
use crate::utils::types::Clock;
use anyhow::Context;
use anyhow::Result;
use futures::StreamExt;
use kube::api::DynamicObject;
use kube::api::GroupVersionKind;
use loole::RecvStream;
use p2panda_core::{Operation, PrivateKey};
use p2panda_store::MemoryStore;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace, warn};

use super::operations::LinkedOperations;
use super::partition::Partition;
use super::topic::InstanceId;
use super::topic::MeshTopicLogMap;
use super::{operations::Extensions, topic::MeshLogId};

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_FORCED_SYNC_MAX_ATTEMPTS: usize = 10;

pub struct MeshActor {
    instance_id: InstanceId,
    network_tx: mpsc::Sender<Operation<Extensions>>,
    network_rx: mpsc::Receiver<Operation<Extensions>>,
    event_rx: RecvStream<KubeEvent>,
    membership_rx: broadcast::Receiver<MembershipEvent>,
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
        topic_log_map: MeshTopicLogMap,
        network_tx: mpsc::Sender<Operation<Extensions>>,
        network_rx: mpsc::Receiver<Operation<Extensions>>,
        event_rx: RecvStream<KubeEvent>,
        membership_rx: broadcast::Receiver<MembershipEvent>,
        subscriptions: Subscriptions,
        store: MemoryStore<MeshLogId, Extensions>,
    ) -> MeshActor {
        let own_log_id = MeshLogId(instance_id.clone());
        let operations = LinkedOperations::new(key.clone(), instance_id.clone());
        let operation_log = OperationLog::new(
            own_log_id.clone(),
            key.public_key(),
            topic_log_map.clone(),
            store.clone(),
        );
        // TODO initialization
        let membership = Membership::default();
        MeshActor {
            last_snapshot_time: clock.now(),
            operations,
            operation_log,
            instance_id,
            network_tx,
            network_rx,
            event_rx,
            membership_rx,
            subscriptions,
            partition,
            clock,
            cancelation,
            snapshot_config,
            tombstone_config,
            own_log_id,
            membership,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut interval = tokio::time::interval(DEFAULT_TICK_INTERVAL);
        loop {
            tokio::select! {
                _ = self.cancelation.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(err) = self.on_tick().await {
                        error!("error on tick, {}", err);
                    }
                },
                Ok(event) = self.membership_rx.recv() => {
                    let MembershipEvent::Update(membership) = event;
                    if let Err(err) = self.on_membership_change(membership).await {
                        error!("error while processing kube event, {}", err);
                    }
                }
                Some(event) = self.event_rx.next() => {
                    if let Err(err) = self.on_outgoing_to_network(event).await {
                        error!("error while processing kube event, {}", err);
                    }
                }
                Some(message) = self.network_rx.recv() => {
                    if let Err(err) = self.on_incoming_from_network(message).await {
                        error!("error while processing network event, {}", err);
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_membership_change(&mut self, membership: Membership) -> Result<()> {
        self.membership = membership;
        let mesh_events = self
            .partition
            .mesh_membership_change(&self.membership, &self.instance_id.zone)?;
        for event in mesh_events {
            let operation = self.operations.next(event);
            self.operation_log.insert(operation).await?;
        }
        self.on_ready().await?;
        Ok(())
    }

    async fn on_outgoing_to_network(&mut self, event: KubeEvent) -> Result<()> {
        self.on_event(event).await?;
        self.on_ready().await?;
        Ok(())
    }

    async fn on_event(&mut self, event: KubeEvent) -> Result<()> {
        let update_result = self.partition.kube_apply(event, &self.instance_id.zone)?;
        let event: Option<MeshEvent> = update_result.into();
        // info!("kube event => mesh event {:?}", event);

        if let Some(event) = event {
            let operation = self.operations.next(event);
            self.operation_log.insert(operation).await?;
        }
        Ok(())
    }

    async fn on_ready(&mut self) -> Result<()> {
        if let Some(mut ready) = self.operation_log.get_ready().await {
            self.on_ready_outgoing(&mut ready).await;
            self.on_ready_incoming(&mut ready).await?;
        }
        Ok(())
    }

    async fn on_incoming_from_network(&mut self, operation: Operation<Extensions>) -> Result<()> {
        self.operation_log.insert(operation).await?;
        self.on_ready().await
    }

    async fn on_ready_incoming(&mut self, ready: &mut Ready) -> Result<(), anyhow::Error> {
        for (log_id, ops) in ready.take_incoming().into_iter() {
            for operation in ops.into_iter() {
                let pointer = operation.header.seq_num;
                if let Some(body) = operation.body {
                    let mesh_event = MeshEvent::try_from(body.to_bytes())?;
                    let merge_results = self.partition.mesh_apply(
                        mesh_event,
                        &log_id.0.zone,
                        &self.instance_id.zone,
                        &self.membership,
                    )?;
                    self.on_merge_results(merge_results).await;
                    self.operation_log.advance_log_pointer(&log_id, pointer + 1);
                } else {
                    error!("event has no body");
                }
            }
        }
        Ok(())
    }

    async fn on_merge_results(&mut self, merge_results: Vec<MergeResult>) {
        for merge_result in merge_results.into_iter() {
            let ok_or_error = self.on_merge_result(merge_result).await;
            if let Err(err) = ok_or_error {
                error!("error while merging {err}");
            }
        }
    }

    async fn on_merge_result(&mut self, merge_result: MergeResult) -> Result<()> {
        let event = if let MergeResult::Update { event, .. } = &merge_result {
            event.clone()
        } else {
            None
        };

        let initial_ok_or_error = self.kube_apply(merge_result).await;

        let final_ok_or_result = match initial_ok_or_error {
            Ok(PersistenceResult::Conflict {
                gvk,
                name,
                operation_type,
            }) => self.forced_sync(gvk, name, operation_type).await,
            Ok(ok) => Ok(ok),
            Err(err) => Err(err),
        };

        if let (Ok(PersistenceResult::Persisted), Some(event)) = (&final_ok_or_result, event) {
            let operation = self.operations.next(event);
            self.operation_log.insert(operation).await?;
        }
        final_ok_or_result.map(|_| ())
    }

    async fn kube_apply(&mut self, merge_result: MergeResult) -> Result<PersistenceResult> {
        match merge_result {
            MergeResult::Create { object } => self.kube_patch_apply(object).await,
            MergeResult::Update { object, .. } => self.kube_patch_apply(object).await,
            MergeResult::Delete(Tombstone {
                gvk,
                name,
                resource_version,
                ..
            }) => self.kube_delete(&gvk, &name, resource_version).await,
            MergeResult::Skip | MergeResult::Tombstone { .. } => Ok(PersistenceResult::Skipped),
        }
    }

    async fn forced_sync(
        &mut self,
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
                if let Err(err) = self.on_event(event).await {
                    error!("on_event error during forced sync {err}");
                }
                self.partition.update_resource_version(&name, version);
                if let Some(current) = self.partition.get(&name) {
                    persistence_result = match operation_type {
                        OperationType::Update => self.kube_patch_apply(current).await?,
                        OperationType::Delete => {
                            self.kube_delete(&gvk, &name, current.get_resource_version())
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
                warn!("object {gvk:?} is no longer present");
                return Ok(PersistenceResult::Skipped);
            }
            attempts -= 1;
        }
        warn!(
            "Conflicts: Number of attempts ({DEFAULT_FORCED_SYNC_MAX_ATTEMPTS}) is exhausted while updating object {}",
            name
        );
        Ok(persistence_result)
    }

    async fn kube_patch_apply(&mut self, mut object: DynamicObject) -> Result<PersistenceResult> {
        let gvk = object.get_gvk()?;
        let name = object.get_namespaced_name();

        let existing = self.subscriptions.client().get(&gvk, &name).await?;

        object.metadata.managed_fields = None;
        if let Some(existing) = existing {
            let existing_version = existing.get_resource_version();
            object.types = existing.types;
            object.metadata.uid = existing.metadata.uid;
            debug!(
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
                Ok(PersistenceResult::Persisted)
            }
            Err(ClientError::VersionConflict) => {
                debug!("Version Conflict for resource {}", name);
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
                    error!(%name, %status.code, %status.message, %status.reason, "delete object failure");
                }
            });
        } else {
            warn!(%name, "Object not found. Skipping delete.");
        }
        Ok(PersistenceResult::Persisted)
    }

    async fn on_ready_outgoing(&mut self, ready: &mut Ready) {
        for operation in ready.take_outgoing().into_iter() {
            let pointer = operation.header.seq_num;
            self.network_tx.send(operation).await.ok();
            self.operation_log
                .advance_log_pointer(&self.own_log_id, pointer + 1);
        }
    }

    async fn on_tick(&mut self) -> Result<()> {
        self.on_ready().await?;
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
            self.send_snapshot().await?;
            self.operation_log.truncate_obsolete_logs().await?;
        }

        self.partition
            .drop_tombstones(self.tombstone_config.tombstone_retention_interval_seconds);

        Ok(())
    }

    async fn send_snapshot(&mut self) -> Result<()> {
        trace!("Periodic snapshot");
        let event = self.partition.get_mesh_snapshot(&self.instance_id.zone);
        let operation = self.operations.next(event);
        self.on_incoming_from_network(operation).await?;
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
    Persisted,
    Conflict {
        gvk: GroupVersionKind,
        name: NamespacedName,
        operation_type: OperationType,
    },
    Skipped,
}
