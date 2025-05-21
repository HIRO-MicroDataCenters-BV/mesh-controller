use std::sync::Arc;

use crate::node::mesh::NodeConfig;
use async_trait::async_trait;
use futures_util::{AsyncRead, AsyncWrite, Sink, SinkExt, StreamExt};
use p2panda_core::{PrivateKey, PublicKey};
use p2panda_sync::cbor::{into_cbor_sink, into_cbor_stream};
use p2panda_sync::{FromSync, SyncError, SyncProtocol};
use rand::random;
use serde::{Deserialize, Serialize};
use tracing::{Level, debug, span};

use super::types::Query;

#[derive(Debug)]
pub struct MeshSyncProtocol {
    config: NodeConfig,
    private_key: PrivateKey,
    public_key: PublicKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Message {
    #[serde(rename = "handshake")]
    Handshake(Query),
}

#[async_trait]
impl<'a> SyncProtocol<'a, Query> for MeshSyncProtocol {
    fn name(&self) -> &'static str {
        "mesh-sync-v1"
    }

    async fn initiate(
        self: Arc<Self>,
        query: Query,
        tx: Box<&'a mut (dyn AsyncWrite + Send + Unpin)>,
        rx: Box<&'a mut (dyn AsyncRead + Send + Unpin)>,
        mut app_tx: Box<&'a mut (dyn Sink<FromSync<Query>, Error = SyncError> + Send + Unpin)>,
    ) -> Result<(), SyncError> {
        let session_id: [u8; 2] = random();
        let span = span!(Level::DEBUG, "initiator", session_id = hex::encode(session_id), %query);

        let mut sink = into_cbor_sink(tx);
        let mut stream = into_cbor_stream::<Message>(rx);

        // Inform p2panda backend about query.
        app_tx
            .send(FromSync::HandshakeSuccess(query.clone()))
            .await?;

        // 1. Send handshake message over to other peer so that remote peer learns what we would
        //    like to sync.
        debug!(parent: &span, "sending sync query {query}");
        sink.send(Message::Handshake(query.clone())).await?;

        // 2. End prematurely when we don't want to sync.
        if query.is_no_sync() {
            debug!(parent: &span, "end sync session prematurely as we don't want to have one");
            return Ok(());
        }

        sink.flush().await?;
        app_tx.flush().await?;

        debug!(parent: &span, "sync session finished");

        Ok(())
    }

    async fn accept(
        self: Arc<Self>,
        tx: Box<&'a mut (dyn AsyncWrite + Send + Unpin)>,
        rx: Box<&'a mut (dyn AsyncRead + Send + Unpin)>,
        mut app_tx: Box<&'a mut (dyn Sink<FromSync<Query>, Error = SyncError> + Send + Unpin)>,
    ) -> Result<(), SyncError> {
        let mut sink = into_cbor_sink::<Message>(tx);
        let mut stream = into_cbor_stream(rx);

        // 1. Expect initiating peer to tell us what they want to sync.
        let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
            "incoming message stream ended prematurely".into(),
        ))??;
        let Message::Handshake(query) = message else {
            return Err(SyncError::UnexpectedBehaviour(
                "did not receive expected message".into(),
            ));
        };

        let session_id: [u8; 2] = random();
        let span = span!(Level::DEBUG, "acceptor", session_id = hex::encode(session_id),  %query);
        debug!(parent: &span, "received sync query {}", query);

        // Tell p2panda backend about query.
        app_tx
            .send(FromSync::HandshakeSuccess(query.clone()))
            .await?;

        // 2. The other peer might tell us sometimes that they _don't_ want to sync.
        if query.is_no_sync() {
            debug!(parent: &span, "end sync session prematurely as we don't want to have one");
            return Ok(());
        }

        // Flush all bytes so that no messages are lost.
        sink.flush().await?;
        app_tx.flush().await?;

        debug!(parent: &span, "sync session finished");

        Ok(())
    }
}

impl MeshSyncProtocol {
    pub fn new(config: NodeConfig, private_key: PrivateKey) -> Self {
        Self {
            config,
            public_key: private_key.public_key(),
            private_key,
        }
    }
}
