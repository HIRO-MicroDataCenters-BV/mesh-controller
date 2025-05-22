use crate::kube::{kube_api::KubeApi, types::CacheProtocol};
use crate::metrics::MESSAGE_RECEIVE_TOTAL;
use crate::network::message::NetworkMessage;
use crate::network::message::NetworkPayload;
use crate::node::mesh::NodeConfig;
use anyhow::{Context, Result, anyhow};
use axum_prometheus::metrics;
use futures_util::stream::SelectAll;
use kube::api::DynamicObject;
use loole::RecvStream;
use p2panda_core::{Hash, PrivateKey, PublicKey};
use p2panda_net::network::FromNetwork;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, trace, warn};

use crate::network::Panda;

pub enum ToNodeActor {
    Shutdown { reply: oneshot::Sender<()> },
}

pub struct MeshNodeActor {
    _config: NodeConfig,
    private_key: PrivateKey,
    public_key: PublicKey,
    kube: KubeApi,
    inbox: mpsc::Receiver<ToNodeActor>,
    kube_consumer_rx: SelectAll<RecvStream<CacheProtocol>>,
    p2panda_topic_rx: SelectAll<ReceiverStream<FromNetwork>>,
    panda: Panda,
}

impl MeshNodeActor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: NodeConfig,
        private_key: PrivateKey,
        panda: Panda,
        kube: KubeApi,
        inbox: mpsc::Receiver<ToNodeActor>,
    ) -> Self {
        Self {
            _config: config,
            public_key: private_key.public_key(),
            private_key,
            inbox,
            kube,
            kube_consumer_rx: SelectAll::new(),
            p2panda_topic_rx: SelectAll::new(),
            panda,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from external API awaited by `shutdown` call and fire it as soon as
        // shutdown completed to signal.
        let shutdown_completed_signal = self.run_inner().await;
        if let Err(err) = self.shutdown().await {
            error!(?err, "error during shutdown");
        }

        drop(self);

        match shutdown_completed_signal {
            Ok(reply_tx) => {
                reply_tx.send(()).ok();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn run_inner(&mut self) -> Result<oneshot::Sender<()>> {
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        ToNodeActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                    }
                },
                Some(event) = self.kube_consumer_rx.next() => {
                    if let Err(err) = self.on_kube_event(event).await {
                        warn!("error during kube event handling: {}", err);
                    }
                },
                Some(event) = self.p2panda_topic_rx.next() => {
                    if let Err(err) = self.on_network_event(event).await {
                        warn!("error during network event handling: {}", err);
                    }
                },
                else => {
                    // Error occurred outside of actor and our select! loop got disabled. We exit
                    // here with an error which will probably be overriden by the external error
                    // which caused the problem in first hand.
                    break Err(anyhow!("all select! branches are disabled"));
                }
            }
        }
    }

    async fn on_kube_event(&mut self, event: CacheProtocol) -> Result<()> {
        debug!(event = %event, "received cache event message, broadcast it in gossip overlay");

        MeshNodeActor::increment_received_local_messages();

        let mesh_topic_id: [u8; 32] = *Hash::new("mesh").as_bytes();

        let mut network_message =
            NetworkMessage::new_resource_msg("test".into(), event, &self.public_key);
        network_message.sign(&self.private_key);

        self.broadcast(network_message, mesh_topic_id).await?;

        Ok(())
    }

    /// Broadcast message in gossip overlay for this topic.
    async fn broadcast(&self, message: NetworkMessage, topic_id: [u8; 32]) -> Result<()> {
        self.panda
            .broadcast(message.to_bytes(), topic_id)
            .await
            .context("broadcast message")?;
        Ok(())
    }

    /// Handler for incoming events from the p2p network.
    ///
    /// These events can come from either gossip broadcast or sync sessions with other peers.
    async fn on_network_event(&mut self, event: FromNetwork) -> Result<()> {
        let (bytes, delivered_from, _is_gossip) = match event {
            FromNetwork::GossipMessage {
                bytes,
                delivered_from,
            } => {
                trace!(
                    source = "gossip",
                    bytes = bytes.len(),
                    "received network message"
                );
                (bytes, delivered_from, true)
            }
            FromNetwork::SyncMessage {
                header,
                delivered_from,
                ..
            } => {
                trace!(
                    source = "sync",
                    bytes = header.len(),
                    "received network message"
                );
                (header, delivered_from, false)
            }
        };

        let network_message = NetworkMessage::from_bytes(&bytes)?;

        // Check the signature
        if !network_message.verify() {
            warn!(
                %delivered_from, public_key = %network_message.public_key,
                "ignored network message with invalid signature"
            );
            return Ok(());
        }

        let _signature = network_message
            .signature
            .expect("signatures was already checked at this point and should be given");

        match &network_message.payload {
            NetworkPayload::ResourceUpdate(source, payload) => {
                let object: DynamicObject =
                    serde_json::from_slice(payload).context("deserialize resource payload")?;

                self.kube.apply_update(source, object).await?;
            }
            NetworkPayload::ResourceSnapshot(source, payload) => {
                let object: DynamicObject =
                    serde_json::from_slice(payload).context("deserialize resource payload")?;

                self.kube.apply_snapshot(source, object).await?;
            }
        }

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.panda.shutdown().await?;
        Ok(())
    }

    fn increment_received_local_messages() {
        metrics::counter!(MESSAGE_RECEIVE_TOTAL,).increment(1);
    }
}
