use std::time::Duration;
use std::time::SystemTime;

use crate::config::configuration::PeriodicSnapshotConfig;
use crate::kube::dynamic_object_ext::DynamicObjectExt;
use crate::kube::event::KubeEvent;
use crate::kube::subscriptions::Subscriptions;
use crate::merge::types::MergeResult;
use crate::mesh::event::MeshEvent;
use crate::mesh::operation_log::OperationLog;
use anyhow::Context;
use anyhow::Result;
use futures::StreamExt;
use loole::RecvStream;
use p2panda_core::{Operation, PrivateKey};
use p2panda_store::MemoryStore;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, trace, warn};

use super::operations::LinkedOperations;
use super::partition::Partition;
use super::topic::InstanceId;
use super::topic::MeshTopicLogMap;
use super::{operations::Extensions, topic::MeshLogId};

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(1);

pub struct MeshActor {
    instance_id: InstanceId,
    network_tx: mpsc::Sender<Operation<Extensions>>,
    network_rx: mpsc::Receiver<Operation<Extensions>>,
    event_rx: RecvStream<KubeEvent>,
    subscriptions: Subscriptions,
    operation_log: OperationLog,
    operations: LinkedOperations,
    partition: Partition,
    cancelation: CancellationToken,
    snapshot_config: PeriodicSnapshotConfig,
    last_snapshot_time: SystemTime,
    own_log_id: MeshLogId,
}

impl MeshActor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key: PrivateKey,
        snapshot_config: PeriodicSnapshotConfig,
        instance_id: InstanceId,
        partition: Partition,
        cancelation: CancellationToken,
        topic_log_map: MeshTopicLogMap,
        network_tx: mpsc::Sender<Operation<Extensions>>,
        network_rx: mpsc::Receiver<Operation<Extensions>>,
        event_rx: RecvStream<KubeEvent>,
        subscriptions: Subscriptions,
        store: MemoryStore<MeshLogId, Extensions>,
    ) -> MeshActor {
        let own_log_id = MeshLogId(instance_id.clone());
        MeshActor {
            operations: LinkedOperations::new(key.clone(), instance_id.clone()),
            operation_log: OperationLog::new(
                own_log_id.clone(),
                key.public_key(),
                topic_log_map.clone(),
                store.clone(),
            ),
            instance_id,
            network_tx,
            network_rx,
            event_rx,
            subscriptions,
            partition,
            cancelation,
            snapshot_config,
            last_snapshot_time: SystemTime::now(),
            own_log_id,
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
                Some(event) = self.event_rx.next() => {
                    if let Err(err) = self.on_outgoing_to_network(event).await {
                        error!("error while processing cache event, {}", err);
                    }
                }
                Some(message) = self.network_rx.recv() => {
                    if let Err(err) = self.on_apply(message).await {
                        error!("error while processing network event, {}", err);
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_outgoing_to_network(&mut self, event: KubeEvent) -> Result<()> {
        let update_result = self.partition.kube_apply(&event, &self.instance_id.zone)?;
        let event: Option<MeshEvent> = update_result.into();

        if let Some(event) = event {
            let operation = self.operations.next(event);
            self.on_apply(operation).await?;
        }
        Ok(())
    }

    async fn on_apply(&mut self, operation: Operation<Extensions>) -> Result<()> {
        self.operation_log.insert(operation.clone()).await?;
        self.on_ready().await
    }

    async fn on_ready(&mut self) -> Result<()> {
        if let Some(mut ready) = self.operation_log.get_ready().await {
            for operation in ready.take_outgoing().into_iter() {
                let pointer = operation.header.seq_num;
                self.network_tx.send(operation).await.ok();
                self.operation_log
                    .advance_log_pointer(&self.own_log_id, pointer + 1);
            }

            for (log_id, ops) in ready.take_incoming().into_iter() {
                for operation in ops.into_iter() {
                    let pointer = operation.header.seq_num;
                    if let Some(body) = operation.body {
                        let mesh_event = MeshEvent::try_from(body.to_bytes())?;
                        let merge_results =
                            self.partition.mesh_apply(mesh_event, &log_id.0.zone)?;
                        for merge_result in merge_results.into_iter() {
                            if let Err(err) = self.handle_result(merge_result).await {
                                error!("error while merging {err}");
                            }
                        }
                        self.operation_log.advance_log_pointer(&log_id, pointer + 1);
                    } else {
                        error!("event has no body");
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn handle_result(&self, merge_result: MergeResult) -> Result<()> {
        match merge_result {
            MergeResult::Create { mut object } | MergeResult::Update { mut object } => {
                let gvk = object.get_gvk()?;
                let name = object.get_namespaced_name();
                if let Some(existing) = self.subscriptions.client().direct_get(&gvk, &name).await? {
                    object.types = existing.types;
                    object.metadata.managed_fields = None;
                    object.metadata.uid = existing.metadata.uid;
                    object.metadata.resource_version = existing.metadata.resource_version;
                    self.subscriptions
                        .client()
                        .direct_patch_apply(object)
                        .await?;
                } else {
                    self.subscriptions
                        .client()
                        .direct_patch_apply(object)
                        .await?;
                }
            }
            MergeResult::Delete { gvk, name } => {
                if self
                    .subscriptions
                    .client()
                    .direct_get(&gvk, &name)
                    .await?
                    .is_some()
                {
                    self.subscriptions
                        .client()
                        .direct_delete(&gvk, &name)
                        .await?;
                } else {
                    warn!("Object not found {name} {gvk:?}. Skipping delete.");
                }
            }
            MergeResult::Conflict { msg } => {
                tracing::warn!("Conflict detected: {}", msg);
            }
            MergeResult::DoNothing => (),
        }
        Ok(())
    }

    async fn on_tick(&mut self) -> Result<()> {
        self.on_ready().await?;
        let truncate_size =
            self.operations.count_since_snapshot() >= self.snapshot_config.snapshot_max_log;
        let snapshot_time = SystemTime::now()
            .duration_since(self.last_snapshot_time)
            .context("compute duration since last snapshot time")?
            .as_secs()
            > self.snapshot_config.snapshot_interval_seconds;
        if truncate_size || snapshot_time {
            self.send_snapshot().await?;
            self.operation_log.truncate_obsolete_logs().await?;
        }
        Ok(())
    }

    async fn send_snapshot(&mut self) -> Result<()> {
        trace!("Periodic snapshot");
        let event = self.partition.mesh_snapshot(&self.instance_id.zone);
        let operation = self.operations.next(event);
        self.on_apply(operation).await?;
        self.last_snapshot_time = SystemTime::now();
        Ok(())
    }
}
