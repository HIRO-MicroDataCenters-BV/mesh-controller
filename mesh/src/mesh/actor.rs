use std::time::Duration;
use std::time::SystemTime;

use crate::config::configuration::PeriodicSnapshotConfig;
use crate::kube::dynamic_object_ext::DynamicObjectExt;
use crate::kube::event::KubeEvent;
use crate::kube::pool::ObjectPool;
use crate::merge::types::MergeResult;
use crate::mesh::event::MeshEvent;
use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use futures::StreamExt;
use loole::RecvStream;
use p2panda_core::PublicKey;
use p2panda_core::{Body, Header};
use p2panda_core::{Operation, PrivateKey};
use p2panda_store::LogStore;
use p2panda_store::MemoryStore;
use p2panda_store::OperationStore;
use p2panda_stream::operation::{IngestResult, ingest_operation};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::{error, info};
use tracing::{trace, warn};

use super::operations::LinkedOperations;
use super::partition::Partition;
use super::topic::InstanceId;
use super::topic::MeshTopicLogMap;
use super::{operations::Extensions, topic::MeshLogId};

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(5);

pub struct MeshActor {
    instance_id: InstanceId,
    topic_log_map: MeshTopicLogMap,
    network_tx: mpsc::Sender<Operation<Extensions>>,
    network_rx: mpsc::Receiver<Operation<Extensions>>,
    event_rx: RecvStream<KubeEvent>,
    pool: ObjectPool,
    store: MemoryStore<MeshLogId, Extensions>,
    operations: LinkedOperations,
    partition: Partition,
    cancelation: CancellationToken,
    snapshot_config: PeriodicSnapshotConfig,
    last_snapshot_time: SystemTime,
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
        pool: ObjectPool,
        store: MemoryStore<MeshLogId, Extensions>,
    ) -> MeshActor {
        MeshActor {
            operations: LinkedOperations::new(key, instance_id.clone()),
            topic_log_map,
            instance_id,
            network_tx,
            network_rx,
            event_rx,
            pool,
            store,
            partition,
            cancelation,
            snapshot_config,
            last_snapshot_time: SystemTime::now(),
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
                    if let Err(err) = self.on_incoming_from_network(message).await {
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
            self.outgoing_to_network(operation).await?;
        }
        Ok(())
    }

    async fn outgoing_to_network(&mut self, operation: Operation<Extensions>) -> Result<()> {
        let Operation {
            hash: _,
            header,
            body,
        } = operation;
        let header_bytes = header.to_bytes();
        let extensions = header
            .extensions
            .as_ref()
            .ok_or(anyhow!("extensions are not set in header"))?;
        let log_id = extensions.log_id.clone();
        let prune_flag = extensions.prune_flag.is_set();

        let result = ingest_operation(
            &mut self.store,
            header,
            body,
            header_bytes,
            &log_id,
            prune_flag,
        )
        .await
        .inspect_err(|e| {
            error!("Error during ingest operation {}", e);
        })?;
        match result {
            IngestResult::Complete(op) => {
                info!(
                    "Outgoing Operation({}, {})",
                    op.header.seq_num, log_id.0.zone
                );
                if let Err(err) = self.network_tx.send(op).await {
                    warn!(
                        "error sending outgoing operation, will be retried via sync {}",
                        err
                    );
                }
            }
            IngestResult::Retry(h, _, _, ops_missing) => {
                error!(
                    "Outgoing Operation({}, {}), missing ops = {ops_missing}. This should never happen.",
                    h.seq_num, log_id.0.zone
                );
            }
        }
        Ok(())
    }

    async fn on_incoming_from_network(&mut self, operation: Operation<Extensions>) -> Result<()> {
        let Operation {
            hash: _,
            header,
            body,
        } = operation;

        let extensions = header
            .extensions
            .as_ref()
            .ok_or(anyhow!("extensions are not set in header"))?;
        let log_id = extensions.log_id.clone();
        let prune_flag = extensions.prune_flag.is_set();

        self.incoming_from_network(header, body, &log_id, prune_flag)
            .await
    }

    async fn incoming_from_network(
        &mut self,
        header: Header<Extensions>,
        body: Option<Body>,
        log_id: &MeshLogId,
        prune_flag: bool,
    ) -> Result<()> {
        let is_obsolete = self.update_log_id(&header.public_key, log_id);
        if is_obsolete {
            debug!(
                "skipping Operation({}, {}), for log {:?}",
                log_id.0.zone, header.seq_num, log_id
            );
            return Ok(());
        }

        let already_exists = self.store.has_operation(header.hash()).await?;
        if already_exists {
            return Ok(());
        }

        let header_bytes = header.to_bytes();
        let result = ingest_operation(
            &mut self.store,
            header,
            body,
            header_bytes,
            log_id,
            prune_flag,
        )
        .await
        .inspect_err(|e| {
            error!("Error during ingest operation {}", e);
        })?;
        match result {
            IngestResult::Complete(op) => {
                debug!(
                    "Incoming Operation({},{})",
                    log_id.0.zone, op.header.seq_num
                );
                if let Some(body) = op.body {
                    let mesh_event = MeshEvent::try_from(body.to_bytes())?;
                    let merge_results = self.partition.mesh_apply(mesh_event, &log_id.0.zone)?;
                    for merge_result in merge_results.into_iter() {
                        if let Err(err) = self.handle_result(merge_result).await {
                            error!("error while merging {err}");
                        }
                    }
                } else {
                    panic!("event has no body");
                }
            }
            IngestResult::Retry(h, _, _, ops_missing) => {
                debug!(
                    "Incoming Operation({},{}), missing ops = {:?}",
                    log_id.0.zone, h.seq_num, ops_missing
                );
            }
        }
        Ok(())
    }

    fn update_log_id(&mut self, incoming_source: &PublicKey, incoming_log_id: &MeshLogId) -> bool {
        match self.topic_log_map.get_latest_log(incoming_source) {
            Some(latest) => match latest.0.start_time.cmp(&incoming_log_id.0.start_time) {
                std::cmp::Ordering::Less => {
                    debug!("new log {} found from peer", incoming_log_id);
                    self.topic_log_map
                        .update_log(incoming_source.to_owned(), incoming_log_id.to_owned());
                    false
                }
                std::cmp::Ordering::Equal => false,
                std::cmp::Ordering::Greater => true,
            },
            None => {
                debug!("new log {} found from peer", incoming_log_id);
                self.topic_log_map
                    .update_log(incoming_source.to_owned(), incoming_log_id.to_owned());
                false
            }
        }
    }

    pub async fn handle_result(&self, merge_result: MergeResult) -> Result<()> {
        match merge_result {
            MergeResult::Create { mut object } | MergeResult::Update { mut object } => {
                let gvk = object.get_gvk()?;
                let name = object.get_namespaced_name();
                if let Some(existing) = self.pool.client().direct_get(&gvk, &name).await? {
                    object.types = existing.types;
                    object.metadata.managed_fields = None;
                    object.metadata.uid = existing.metadata.uid;
                    object.metadata.resource_version = existing.metadata.resource_version;
                    self.pool.client().direct_patch_apply(object).await?;
                } else {
                    self.pool.client().direct_patch_apply(object).await?;
                }
            }
            MergeResult::Delete { gvk, name } => {
                if self.pool.client().direct_get(&gvk, &name).await?.is_some() {
                    self.pool.client().direct_delete(&gvk, &name).await?;
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
        let truncate_size =
            self.operations.count_since_snapshot() >= self.snapshot_config.snapshot_max_log;
        let truncate_time = SystemTime::now()
            .duration_since(self.last_snapshot_time)
            .context("compute duration since last snapshot time")?
            .as_secs()
            > self.snapshot_config.snapshot_interval_seconds;
        if truncate_size || truncate_time {
            self.send_snapshot().await?;
            self.truncate_obsolete_logs().await?;
        }
        Ok(())
    }

    async fn send_snapshot(&mut self) -> Result<()> {
        trace!("Periodic snapshot");
        let event = self.partition.mesh_snapshot(&self.instance_id.zone);
        let operation = self.operations.next(event);
        self.outgoing_to_network(operation).await?;
        self.last_snapshot_time = SystemTime::now();
        Ok(())
    }

    async fn truncate_obsolete_logs(&mut self) -> Result<()> {
        for (source, log_ids) in self.topic_log_map.take_obsolete_log_ids() {
            for log_id in log_ids {
                if let Some((header, _)) = self.store.latest_operation(&source, &log_id).await? {
                    trace!("Truncating log {log_id:?}");
                    if let Err(err) = self
                        .store
                        .delete_operations(&source, &log_id, header.seq_num)
                        .await
                    {
                        error!("Error while truncating log {log_id:?}: {err}");
                    }
                }
            }
        }
        Ok(())
    }
}
