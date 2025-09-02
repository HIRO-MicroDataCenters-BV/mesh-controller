use anyhow::Result;

use meshkube::client::KubeClient;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{meshpeer::MeshPeerExt, peers::Peers, types::PeerUpdate};

pub enum ToNodeActor {
    MeshPeerUpdate(PeerUpdate),
}

pub struct MeshPeerResourceActor {
    inbox: mpsc::Receiver<ToNodeActor>,
    cancelation: CancellationToken,
    client: KubeClient,
    peers: Peers,
}

impl MeshPeerResourceActor {
    pub fn new(
        inbox: mpsc::Receiver<ToNodeActor>,
        client: KubeClient,
        cancelation: CancellationToken,
    ) -> MeshPeerResourceActor {
        MeshPeerResourceActor {
            peers: Peers::new(),
            client,
            inbox,
            cancelation,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                Some(message) = self.inbox.recv() => {
                    if let Err(err) = self.on_actor_message(message).await {
                        error!("MeshPeerResourceActor: error updating mesh peer state {err}");
                    }
                },
                _ = self.cancelation.cancelled() => break,
            }
        }
        Ok(())
    }

    async fn on_actor_message(&mut self, message: ToNodeActor) -> Result<()> {
        match message {
            ToNodeActor::MeshPeerUpdate(update) => self.on_peer_update(update).await,
        }
    }

    async fn on_peer_update(&mut self, update: PeerUpdate) -> Result<()> {
        let peer = self.peers.update_and_get(update);
        let object = peer.to_owned().to_object()?;
        self.client.patch_apply(object).await?;
        Ok(())
    }
}
