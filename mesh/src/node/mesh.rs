use std::net::SocketAddr;

use crate::JoinErrToStr;
use crate::config::configuration::Config;
use crate::logs::kube_api::KubeApi;
use crate::logs::topic::MeshTopic;
use crate::network::Panda;
use anyhow::{Context, Result, anyhow};
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use p2panda_core::{Hash, PrivateKey, PublicKey};
use p2panda_net::Config as NetworkConfig;
use p2panda_net::NodeAddress;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, warn};

use super::actor::{MeshNodeActor, ToNodeActor};

#[derive(Debug, Clone, Default)]
pub struct NodeOptions {
    pub public_key: PublicKey,
    pub private_key: PrivateKey,
    pub direct_addresses: Vec<SocketAddr>,
    pub node_config: NodeConfig,
}

#[derive(Debug, Clone, Default)]
pub struct NodeConfig {}

pub struct MeshNode {
    #[allow(dead_code)]
    node_id: PublicKey,
    #[allow(dead_code)]
    direct_addresses: Vec<SocketAddr>,
    node_actor_tx: mpsc::Sender<ToNodeActor>,
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl MeshNode {
    pub fn new(panda: Panda, kube: KubeApi, options: NodeOptions) -> Result<Self> {
        let (node_actor_tx, node_actor_rx) = mpsc::channel(512);
        let node_actor = MeshNodeActor::new(
            options.node_config,
            options.private_key,
            panda,
            kube,
            node_actor_rx,
        );
        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = node_actor.run().await {
                error!("node actor failed: {err:?}");
            }
        });

        let actor_drop_handle = AbortOnDropHandle::new(actor_handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        let node = MeshNode {
            node_id: options.public_key,
            direct_addresses: options.direct_addresses,
            node_actor_tx,
            actor_handle: actor_drop_handle,
        };

        Ok(node)
    }

    pub async fn configure_p2p_network(
        config: &Config,
    ) -> Result<(NodeConfig, NetworkConfig), anyhow::Error> {
        let node_config = NodeConfig::default();
        let network_id_hash = Hash::new(config.node.network_id.as_bytes());
        let network_id = network_id_hash.as_bytes();
        let mut network_config = NetworkConfig {
            bind_port_v4: config.node.bind_port,
            network_id: *network_id,
            ..Default::default()
        };
        for node in &config.node.known_nodes {
            // Resolve FQDN strings into IP addresses.
            let mut direct_addresses = Vec::new();
            for addr in &node.direct_addresses {
                let maybe_peers = tokio::net::lookup_host(addr)
                    .await
                    .context(format!("Unable to lookup host {:?}, skipping", addr));
                match maybe_peers {
                    Ok(peers) => {
                        for resolved in peers {
                            direct_addresses.push(resolved);
                        }
                    }
                    Err(err) => {
                        warn!("{:?}", err);
                    }
                }
            }
            let node_address = NodeAddress {
                public_key: node.public_key,
                direct_addresses,
                relay_url: None,
            };
            network_config.direct_node_addresses.push(node_address);
        }
        Ok((node_config, network_config))
    }

    pub async fn subscribe(&self, topic: MeshTopic) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.node_actor_tx
            .send(ToNodeActor::Subscribe { topic, reply })
            .await?;
        reply_rx.await?
    }

    pub async fn publish(&self, topic: MeshTopic) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.node_actor_tx
            .send(ToNodeActor::Publish { topic, reply })
            .await?;
        reply_rx.await?
    }

    /// Graceful shutdown of the rhio node.
    pub async fn shutdown(self) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.node_actor_tx
            .send(ToNodeActor::Shutdown { reply })
            .await?;
        reply_rx.await?;
        self.actor_handle.await.map_err(|err| anyhow!("{err}"))?;
        Ok(())
    }
}
