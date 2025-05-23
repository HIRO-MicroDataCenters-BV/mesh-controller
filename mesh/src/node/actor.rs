use crate::kube::types::CacheProtocol;
use crate::logs::kube_api::KubeApi;
use crate::logs::kube_api::MeshLogId;
use crate::logs::topic::MeshTopic;
use crate::metrics::MESSAGE_RECEIVE_TOTAL;
use crate::network::message::NetworkMessage;
use crate::node::mesh::NodeConfig;
use anyhow::{Context, Result, anyhow};
use axum_prometheus::metrics;
use futures_util::stream::SelectAll;
use loole::RecvStream;
use p2panda_core::Body;
use p2panda_core::Header;
use p2panda_core::{PrivateKey, PublicKey};
use p2panda_net::TopicId;
use p2panda_net::network::FromNetwork;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, trace, warn};

use crate::network::Panda;

pub enum ToNodeActor {
    Shutdown {
        reply: oneshot::Sender<()>,
    },
    Subscribe {
        topic: MeshTopic,
        reply: oneshot::Sender<Result<()>>,
    },
    Publish {
        topic: MeshTopic,
        reply: oneshot::Sender<Result<()>>,
    },
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
                        msg => {
                            self.on_actor_message(msg).await;
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

    async fn on_actor_message(&mut self, msg: ToNodeActor) {
        match msg {
            ToNodeActor::Subscribe { topic, reply } => {
                let result = self.on_subscribe(topic).await;
                reply.send(result).ok();
            }
            ToNodeActor::Publish { topic, reply } => {
                let result = self.on_publish(topic).await;
                reply.send(result).ok();
            }
            ToNodeActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }
    }

    async fn on_subscribe(&mut self, topic: MeshTopic) -> Result<()> {
        let network_rx = self
            .panda
            .subscribe(topic)
            .await?
            .expect("queries for subscriptions should always return channel");
        self.p2panda_topic_rx.push(ReceiverStream::new(network_rx));

        Ok(())
    }

    async fn on_publish(&mut self, topic: MeshTopic) -> Result<()> {
        let network_rx = self.panda.subscribe(topic).await?;

        if let Some(network_rx) = network_rx {
            self.p2panda_topic_rx.push(ReceiverStream::new(network_rx));
        }

        Ok(())
    }

    async fn on_kube_event(&mut self, event: CacheProtocol) -> Result<()> {
        debug!(event = %event, "received cache event message, broadcast it in gossip overlay");

        MeshNodeActor::increment_received_local_messages();

        let mesh_topic_id: [u8; 32] = MeshTopic::new("resources").id();

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
        let (header_bytes, payload_bytes, _delivered_from, _is_gossip) = match event {
            FromNetwork::GossipMessage {
                bytes,
                delivered_from,
            } => {
                trace!(
                    source = "gossip",
                    bytes = bytes.len(),
                    "received network message"
                );
                (bytes, None, delivered_from, true)
            }
            FromNetwork::SyncMessage {
                header,
                delivered_from,
                payload,
            } => {
                trace!(
                    source = "sync",
                    bytes = header.len(),
                    "received network message"
                );
                (header, payload, delivered_from, false)
            }
        };
        let header: Header<()> =
            Header::try_from(&header_bytes[..]).context("Header deserialization")?;
        let body: Option<Body> = payload_bytes.map(|b| Body::new(&b));

        self.kube
            .ingest(header, body, header_bytes, &MeshLogId())
            .await?;

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
