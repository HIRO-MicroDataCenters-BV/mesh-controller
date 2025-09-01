use anyhow::Result;

use meshkube::client::KubeClient;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::types::PeerUpdate;

pub enum ToNodeActor {
    MeshPeerUpdate(PeerUpdate),
}

pub struct MeshPeerActor {
    inbox: mpsc::Receiver<ToNodeActor>,
    cancelation: CancellationToken,
    client: KubeClient,
}

impl MeshPeerActor {
    pub fn new(
        inbox: mpsc::Receiver<ToNodeActor>,
        client: KubeClient,
        cancelation: CancellationToken,
    ) -> MeshPeerActor {
        MeshPeerActor {
            client,
            inbox,
            cancelation,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                Some(message) = self.inbox.recv() => {
                    self.on_actor_message(message).await;
                },
                _ = self.cancelation.cancelled() => break,
            }
        }
        Ok(())
    }

    async fn on_actor_message(&mut self, message: ToNodeActor) {
        match message {
            ToNodeActor::MeshPeerUpdate(update) => self.on_peer_update(update),
        }
    }

    fn on_peer_update(&mut self, update: PeerUpdate) {}
}
