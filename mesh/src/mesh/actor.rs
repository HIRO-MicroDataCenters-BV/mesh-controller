use crate::kube::pool::ObjectPool;
use crate::kube::dynamic_object_ext::DynamicObjectExt;
use crate::kube::event::KubeEvent;
use crate::merge::types::MergeResult;
use crate::mesh::event::MeshEvent;
use anyhow::Result;
use anyhow::anyhow;
use futures::StreamExt;
use loole::RecvStream;
use p2panda_core::PublicKey;
use p2panda_core::{Body, Header};
use p2panda_core::{Operation, PrivateKey};
use p2panda_store::MemoryStore;
use p2panda_stream::operation::{IngestResult, ingest_operation};
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing::{trace, warn};

use super::operations::LinkedOperations;
use super::partition::Partition;
use super::topic::InstanceId;
use super::topic::MeshTopicLogMap;
use super::{operations::Extensions, topic::MeshLogId};

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
}

impl MeshActor {
    pub fn new(
        key: PrivateKey,
        instance_id: InstanceId,
        partition: Partition,
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
        }
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                Some(event) = self.event_rx.next() => {
                    if let Err(err) = self.on_outgoing_to_network(event).await {
                        error!("error while processing cache event {}", err);
                    }
                }
                Some(message) = self.network_rx.recv() => {
                    if let Err(err) = self.on_incoming_from_network(message).await {
                        error!("error while processing network event {}", err);
                    }
                }

            }
        }
        // Ok(())
    }

    async fn on_outgoing_to_network(&mut self, event: KubeEvent) -> Result<()> {
        let update_result = self.partition.apply_kube(&event, &self.instance_id.zone)?;
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
        trace!(
            "Outgoing operation: seq_num {}, hash {}",
            header.seq_num,
            header.hash().to_hex(),
        );
        let log_id = header
            .extensions
            .as_ref()
            .ok_or(anyhow!("extensions are not set in header"))?
            .log_id
            .clone();
        let prune_flag = header
            .extensions
            .as_ref()
            .map(|e| e.prune_flag.is_set())
            .unwrap_or(false);

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
                    "Outgoing operation ingest completed: seq_num {}, hash {}, log_id {}",
                    op.header.seq_num,
                    op.header.hash().to_hex(),
                    log_id
                );
                if let Err(err) = self.network_tx.send(op).await {
                    warn!(
                        "error sending outgoing operation, will be retried via sync {}",
                        err
                    );
                }
            }
            IngestResult::Retry(_, _, _, ops_missing) => {
                error!(
                    "Outgoing operation retry: missing ops = {ops_missing}. This should never happen."
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
        let log_id = header
            .extensions
            .as_ref()
            .ok_or(anyhow!("extensions are not set in header"))?
            .log_id
            .clone();
        let prune_flag = header
            .extensions
            .as_ref()
            .map(|e| e.prune_flag.is_set())
            .unwrap_or(false);

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
        if self.obsolete_log(&header.public_key, log_id) {
            trace!(
                "skipping operation {}, for log {}",
                header.hash().to_hex(),
                log_id
            );
            return Ok(());
        }
        trace!(
            "Incoming operation: seq_num {}, hash {}",
            header.seq_num,
            header.hash().to_hex(),
        );
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
                info!(
                    "Incoming operation ingest completed: seq_num {}, hash {}",
                    op.header.seq_num,
                    op.header.hash().to_hex(),
                );
                if let Some(body) = op.body {
                    let mesh_event = MeshEvent::try_from(body.to_bytes())?;
                    let merge_results = self.partition.apply_mesh(mesh_event, &log_id.0.zone)?;
                    for merge_result in merge_results.into_iter() {
                        if let Err(err) = self.handle_result(merge_result).await {
                            error!("error while merging {err}");
                        }
                    }
                } else {
                    panic!("event has no body");
                }
            }
            IngestResult::Retry(_, _, _, ops_missing) => {
                info!("Incoming operation retry: missing ops = {:?}", ops_missing);
            }
        }
        Ok(())
    }

    fn obsolete_log(&self, source: &PublicKey, log_id: &MeshLogId) -> bool {
        match self.topic_log_map.get_latest_log(&source) {
            Some(latest) => {
                if latest.0.start_time < log_id.0.start_time {
                    info!("new log {} found from peer", log_id);
                    self.topic_log_map
                        .update_new_log(source.to_owned(), log_id.to_owned());
                    false
                } else if latest.0.start_time > log_id.0.start_time {
                    true
                } else {
                    false
                }
            }
            None => {
                info!("new log {} found from peer", log_id);
                self.topic_log_map
                    .update_new_log(source.to_owned(), log_id.to_owned());
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
                if let Some(_) = self.pool.client().direct_get(&gvk, &name).await? {
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
}
