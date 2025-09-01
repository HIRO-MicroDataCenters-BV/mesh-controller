use crate::mesh::operations::Extensions;
use crate::mesh::topic::MeshTopic;
use crate::metrics::MESSAGE_RECEIVE_TOTAL;
use crate::network::message::NetworkMessage;
use crate::network::message::NetworkPayload;
use anyhow::{Context, Result, anyhow};
use axum_prometheus::metrics;
use futures_util::stream::SelectAll;
use p2panda_core::Body;
use p2panda_core::Header;
use p2panda_core::Operation;
use p2panda_core::cbor::decode_cbor;
use p2panda_net::TopicId;
use p2panda_net::network::FromNetwork;
use std::io::Cursor;
use tokio::sync::broadcast;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, trace, warn};

use crate::network::Panda;

pub enum ToNodeActor {
    Shutdown {
        reply: oneshot::Sender<()>,
    },
    SubscribeTopic {
        topic: MeshTopic,
        reply: oneshot::Sender<Result<()>>,
    },
    SubscribeNetwork {
        reply: oneshot::Sender<broadcast::Receiver<Operation<Extensions>>>,
    },
    PublishNetwork {
        receiver: broadcast::Receiver<Operation<Extensions>>,
        reply: oneshot::Sender<Result<()>>,
    },
}

pub struct MeshNodeActor {
    inbox: mpsc::Receiver<ToNodeActor>,
    network_rx: SelectAll<BroadcastStream<Operation<Extensions>>>,
    network_tx: Option<tokio::sync::broadcast::Sender<Operation<Extensions>>>,
    p2panda_topic_rx: SelectAll<ReceiverStream<FromNetwork>>,
    panda: Panda,
}

impl MeshNodeActor {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(panda: Panda, inbox: mpsc::Receiver<ToNodeActor>) -> Self {
        MeshNodeActor {
            inbox,
            network_rx: SelectAll::new(),
            network_tx: None,
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
                Some(Ok(operation)) = self.network_rx.next() => {
                    if let Err(err) = self.on_operation(operation).await {
                        warn!("error during operation handling: {}", err);
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
            ToNodeActor::SubscribeTopic { topic, reply } => {
                let result = self.on_subscribe(topic).await;
                reply.send(result).ok();
            }
            ToNodeActor::SubscribeNetwork { reply } => {
                let network_rx = self.network_rx();
                reply.send(network_rx).ok();
            }
            ToNodeActor::PublishNetwork { receiver, reply } => {
                self.network_rx.push(BroadcastStream::new(receiver));
                reply.send(Ok(())).ok();
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

    fn network_rx(&mut self) -> tokio::sync::broadcast::Receiver<Operation<Extensions>> {
        if let Some(network_tx) = &self.network_tx {
            network_tx.subscribe()
        } else {
            let (network_tx, network_rx) = tokio::sync::broadcast::channel(512);
            self.network_tx = Some(network_tx);
            network_rx
        }
    }

    async fn on_operation(&mut self, operation: Operation<Extensions>) -> Result<()> {
        debug!(operation = %operation.hash, "received operation, broadcast it in gossip overlay");

        MeshNodeActor::increment_received_local_messages();

        let topic = MeshTopic::default();
        let mesh_topic_id: [u8; 32] = topic.id();

        self.broadcast(operation, mesh_topic_id).await?;

        Ok(())
    }

    /// Broadcast message in gossip overlay for this topic.
    async fn broadcast(&self, operation: Operation<Extensions>, topic_id: [u8; 32]) -> Result<()> {
        let message = NetworkMessage::new(operation);
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
        let (header, body, _is_gossip) = match event {
            FromNetwork::GossipMessage {
                bytes,
                delivered_from: _,
            } => {
                trace!(
                    source = "gossip",
                    bytes = bytes.len(),
                    "received network message"
                );
                let message =
                    NetworkMessage::from_bytes(&bytes).context("message deserialization")?;
                let NetworkPayload::Operation(header, body) = message.payload;
                (header, body, true)
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
                let header: Header<Extensions> =
                    decode_cbor(Cursor::new(&header_bytes)).context("Header deserialization")?;
                let body: Option<Body> = payload.map(|b| Body::new(&b));
                (header, body, false)
            }
        };
        let operation = Operation {
            hash: header.hash(),
            header: header.clone(),
            body: body.clone(),
        };

        self.subscribers_send(operation);

        Ok(())
    }

    fn subscribers_send(&mut self, operation: Operation<Extensions>) {
        if let Some(subscribers_tx) = &mut self.network_tx {
            subscribers_tx.send(operation).ok();
        }
    }

    async fn shutdown(&self) -> Result<()> {
        self.panda.shutdown().await?;
        Ok(())
    }

    fn increment_received_local_messages() {
        metrics::counter!(MESSAGE_RECEIVE_TOTAL,).increment(1);
    }
}
