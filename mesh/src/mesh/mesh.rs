use std::sync::Arc;

use super::actor::MeshActor;
use super::partition::Partition;
use super::topic::InstanceId;
use super::{operations::Extensions, topic::MeshLogId};
use crate::client::kube_client::KubeClient;
use crate::config::configuration::MergeStrategyType;
use crate::config::configuration::MeshConfig;
use crate::merge::anyapplication_strategy::AnyApplicationMerge;
use crate::merge::default_strategy::DefaultMerge;
use crate::mesh::topic::MeshTopic;
use crate::network::discovery::nodes::Nodes;
use crate::utils::types::Clock;
use crate::{JoinErrToStr, kube::subscriptions::Subscriptions};
use anyhow::Result;
use futures::future::{MapErr, Shared};
use futures::{FutureExt, TryFutureExt};
use p2panda_core::{Operation, PrivateKey};
use p2panda_net::SystemEvent;
use p2panda_store::MemoryStore;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::error;

pub struct Mesh {
    #[allow(dead_code)]
    handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl Mesh {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        key: PrivateKey,
        config: &MeshConfig,
        instance_id: InstanceId,
        cancelation: CancellationToken,
        client: KubeClient,
        clock: Arc<dyn Clock>,
        nodes: Nodes,
        store: MemoryStore<MeshLogId, Extensions>,
        network_tx: mpsc::Sender<Operation<Extensions>>,
        network_rx: mpsc::Receiver<Operation<Extensions>>,
        system_events: broadcast::Receiver<SystemEvent<MeshTopic>>,
    ) -> Result<Mesh> {
        let gvk = config.resource.get_gvk();

        let partition = match config.resource.merge_strategy {
            MergeStrategyType::Default => {
                Partition::new(DefaultMerge::new(gvk.to_owned()), clock.clone())
            }
            MergeStrategyType::AnyApplication => {
                Partition::new(AnyApplicationMerge::new(), clock.clone())
            }
        };
        let subscriptions = Subscriptions::new(client);
        let (subscriber_rx, _) = subscriptions
            .subscribe(&gvk, &config.resource.namespace)
            .await?;
        let actor = MeshActor::new(
            key,
            config.snapshot.to_owned(),
            config.tombstone.to_owned(),
            instance_id,
            partition,
            clock,
            cancelation,
            nodes,
            network_tx,
            network_rx,
            subscriber_rx.into_stream(),
            system_events,
            subscriptions,
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
