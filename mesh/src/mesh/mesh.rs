use std::pin::Pin;
use std::sync::Arc;

use super::actor::MeshActor;
use super::partition::Partition;
use super::topic::InstanceId;
use super::{operations::Extensions, topic::MeshLogId};
use crate::JoinErrToStr;
use crate::config::configuration::MergeStrategyType;
use crate::config::configuration::MeshConfig;
use crate::merge::anyapplication_strategy::AnyApplicationMerge;
use crate::merge::default_strategy::DefaultMerge;
use crate::mesh::actor::ToNodeActor;
use crate::mesh::topic::MeshTopic;
use crate::network::discovery::nodes::Nodes;
use crate::utils::types::Clock;
use anyhow::Result;
use anyhow::anyhow;
use futures::future::{MapErr, Shared};
use futures::{FutureExt, TryFutureExt};
use meshkube::client::KubeClient;
use meshkube::kube::event::KubeEvent;
use p2panda_core::{Operation, PrivateKey};
use p2panda_net::SystemEvent;
use p2panda_store::MemoryStore;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::error;

pub struct Mesh {
    #[allow(dead_code)]
    handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
    mesh_actor_tx: mpsc::Sender<ToNodeActor>,
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
        let (mesh_actor_tx, mesh_actor_rx) = mpsc::channel(512);
        let actor = MeshActor::new(
            key,
            config.snapshot.to_owned(),
            config.tombstone.to_owned(),
            instance_id,
            client,
            partition,
            clock,
            cancelation,
            nodes,
            mesh_actor_rx,
            system_events,
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

        Ok(Mesh {
            handle,
            mesh_actor_tx,
        })
    }

    pub async fn kube_subscribe(
        &self,
        subscriber_rx: Pin<Box<dyn futures::Stream<Item = KubeEvent> + Send + Sync + 'static>>,
    ) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.mesh_actor_tx
            .send(ToNodeActor::KubeSubscribe {
                subscriber_rx,
                reply,
            })
            .await?;
        reply_rx.await?
    }

    pub async fn connect_network(
        &self,
        incoming: broadcast::Receiver<Operation<Extensions>>,
    ) -> Result<broadcast::Receiver<Operation<Extensions>>> {
        let (reply, reply_rx) = oneshot::channel();
        self.mesh_actor_tx
            .send(ToNodeActor::ConnectNetwork { incoming, reply })
            .await?;
        reply_rx.await.map_err(|e| anyhow!("{e}"))
    }
}
