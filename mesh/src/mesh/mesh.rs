use super::actor::MeshActor;
use super::partition::Partition;
use super::topic::InstanceId;
use super::topic::MeshTopicLogMap;
use super::{operations::Extensions, topic::MeshLogId};
use crate::config::configuration::{MergeStrategyType, ResourceConfig};
use crate::merge::anyapplication_strategy::AnyApplicationMerge;
use crate::merge::default_strategy::DefaultMerge;
use crate::{JoinErrToStr, kube::pool::ObjectPool};
use anyhow::Result;
use futures::future::{MapErr, Shared};
use futures::{FutureExt, TryFutureExt};
use p2panda_core::{Operation, PrivateKey};
use p2panda_store::MemoryStore;
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio_util::task::AbortOnDropHandle;
use tracing::error;

pub struct Mesh {
    #[allow(dead_code)]
    handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl Mesh {
    pub async fn new(
        key: PrivateKey,
        config: &ResourceConfig,
        instance_id: InstanceId,
        pool: ObjectPool,
        topic_log_map: MeshTopicLogMap,
        store: MemoryStore<MeshLogId, Extensions>,
        network_tx: mpsc::Sender<Operation<Extensions>>,
        network_rx: mpsc::Receiver<Operation<Extensions>>,
    ) -> Result<Mesh> {
        let gvk = config.get_gvk();
        let namespace = &config.namespace;
        let partition = match config.merge_strategy {
            MergeStrategyType::Default => Partition::new(DefaultMerge::new(gvk.to_owned())),
            MergeStrategyType::AnyApplication => Partition::new(AnyApplicationMerge::new()),
        };
        let event_rx = pool.subscribe(&gvk, &namespace).await?.into_stream();
        let actor = MeshActor::new(
            key,
            instance_id,
            partition,
            topic_log_map,
            network_tx,
            network_rx,
            event_rx,
            pool,
            store,
        );
        let handle = tokio::spawn(async {
            if let Err(error) = actor.run().await {
                error!("mesh actor exited with {error}")
            }
        });
        let handle = AbortOnDropHandle::new(handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        Ok(Mesh { handle })
    }
}
