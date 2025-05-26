use crate::logs::kube_api::KubeApi;
use crate::logs::kube_api::MeshLogId;
use crate::logs::operations::BoxedOperationStream;
use crate::logs::operations::Extensions;
use crate::logs::operations::KubeOperation;
use crate::logs::topic::MeshTopic;
use crate::metrics::MESSAGE_RECEIVE_TOTAL;
use crate::network::message::NetworkMessage;
use crate::network::message::NetworkPayload;
use crate::node::mesh::NodeConfig;
use anyhow::{Context, Result, anyhow};
use axum_prometheus::metrics;
use futures_util::stream::SelectAll;
use p2panda_core::Body;
use p2panda_core::Header;
use p2panda_core::cbor::decode_cbor;
use p2panda_core::Operation;
use p2panda_core::{PrivateKey, PublicKey};
use p2panda_net::TopicId;
use p2panda_net::network::FromNetwork;
use std::io::Cursor;
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
    _private_key: PrivateKey,
    _public_key: PublicKey,
    kube: KubeApi,
    inbox: mpsc::Receiver<ToNodeActor>,
    kube_operations_rx: SelectAll<BoxedOperationStream>,
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
        from_kube: BoxedOperationStream,
        inbox: mpsc::Receiver<ToNodeActor>,
    ) -> Self {
        let mut kube_operations_rx = SelectAll::new();
        kube_operations_rx.push(from_kube);
        Self {
            _config: config,
            _public_key: private_key.public_key(),
            _private_key: private_key,
            inbox,
            kube,
            kube_operations_rx,
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
                Some(operation) = self.kube_operations_rx.next() => {
                    if let Err(err) = self.on_kube_operation(operation).await {
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

    async fn on_kube_operation(&mut self, operation: KubeOperation) -> Result<()> {
        debug!(operation = %operation, "received operation, broadcast it in gossip overlay");

        MeshNodeActor::increment_received_local_messages();

        let mesh_topic_id: [u8; 32] = MeshTopic::new("resources").id();

        let KubeOperation { panda_op , .. } = operation;

        self.broadcast(panda_op, mesh_topic_id).await?;

        Ok(())
    }

    /// Broadcast message in gossip overlay for this topic.
    async fn broadcast(&self, operation: Operation<Extensions>, topic_id: [u8; 32]) -> Result<()> {
        let message = NetworkMessage::new("source",  operation);
        self.panda
            .broadcast(message.to_bytes(), topic_id)
            .await
            .inspect_err(|e| {
                error!(?e, "failed to broadcast message");
            })
            .context("broadcast message")?;
        Ok(())
    }

    /// Handler for incoming events from the p2p network.
    ///
    /// These events can come from either gossip broadcast or sync sessions with other peers.
    async fn on_network_event(&mut self, event: FromNetwork) -> Result<()> {
        let (header, header_bytes, body, _is_gossip) = match event {
            FromNetwork::GossipMessage {
                bytes,
                delivered_from: _,
            } => {
                trace!(
                    source = "gossip",
                    bytes = bytes.len(),
                    "received network message"
                );
                let message = NetworkMessage::from_bytes(&bytes).context("message deserialization")?;
                let NetworkPayload::Operation(_, header, body) = message.payload;
                let header_bytes = header.to_bytes();
                (header, header_bytes, body, true)
            }
            FromNetwork::SyncMessage {
                header: header_bytes,
                delivered_from: _,
                payload,
            } => {
                trace!(
                    source = "sync",
                    bytes = header_bytes.len(),
                    "received network message"
                );
                let header: Header<Extensions> = decode_cbor(Cursor::new(&header_bytes)).context("Header deserialization")?;
                let body: Option<Body> = payload.map(|b| Body::new(&b));
                (header, header_bytes, body, false)
            }
        };


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
