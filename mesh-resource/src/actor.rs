use anyhow::Result;

use crate::{
    meshpeer::{MeshPeer, MeshPeerExt},
    peers::Peers,
    types::PeerState,
};
use anyhow::anyhow;
use kube::api::GroupVersionKind;
use meshkube::client::KubeClient;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::error;

#[derive(Debug)]
pub enum ToNodeActor {
    MeshPeerUpdates(Vec<PeerState>),
    GetAll {
        reply: oneshot::Sender<Result<Vec<PeerState>>>,
    },
}

pub struct MeshPeerResourceActor {
    inbox: mpsc::Receiver<ToNodeActor>,
    cancelation: CancellationToken,
    client: KubeClient,
    peers: Peers,
}

impl MeshPeerResourceActor {
    pub async fn new(
        inbox: mpsc::Receiver<ToNodeActor>,
        client: KubeClient,
        cancelation: CancellationToken,
    ) -> Result<MeshPeerResourceActor> {
        let existing = MeshPeerResourceActor::load_from_store(client.to_owned()).await?;
        Ok(MeshPeerResourceActor {
            peers: Peers::from(existing),
            client,
            inbox,
            cancelation,
        })
    }

    pub async fn load_from_store(client: KubeClient) -> Result<Vec<MeshPeer>> {
        let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "MeshPeer");
        let items = client.list(&gvk, &Some("system".into())).await?;
        items
            .into_iter()
            .map(|object| object.try_parse().map_err(|e| anyhow!("{e}")))
            .collect()
    }

    pub async fn run(mut self) {
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
    }

    async fn on_actor_message(&mut self, message: ToNodeActor) -> Result<()> {
        match message {
            ToNodeActor::MeshPeerUpdates(updates) => self.on_peer_updates(updates).await,
            ToNodeActor::GetAll { reply } => self.on_get_all(reply),
        }
    }

    async fn on_peer_update(&mut self, update: PeerState) -> Result<()> {
        let peer = self.peers.update_and_get(update);
        let object = peer.to_owned().to_object()?;
        self.client.patch_apply(object).await?;
        Ok(())
    }

    async fn on_peer_updates(&mut self, updates: Vec<PeerState>) -> Result<()> {
        for update in updates {
            self.on_peer_update(update).await?;
        }
        Ok(())
    }

    fn on_get_all(&self, reply: oneshot::Sender<Result<Vec<PeerState>>>) -> Result<()> {
        reply.send(Ok(self.peers.get_all())).ok();
        Ok(())
    }
}
