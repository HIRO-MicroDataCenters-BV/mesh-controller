use crate::{
    JoinErrToStr,
    actor::{MeshPeerResourceActor, ToNodeActor},
    types::PeerState,
};
use anyhow::Result;
use futures_util::future::MapErr;
use futures_util::{FutureExt, TryFutureExt, future::Shared};
use meshkube::client::KubeClient;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

pub struct MeshStatus {
    #[allow(dead_code)]
    actor_drop_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
    actor_tx: mpsc::Sender<ToNodeActor>,
}

impl MeshStatus {
    pub async fn new(client: KubeClient, cancelation: CancellationToken) -> Result<MeshStatus> {
        let (actor_tx, actor_rx) = mpsc::channel(512);
        let actor = MeshPeerResourceActor::new(actor_rx, client, cancelation).await?;

        let handle = tokio::spawn(async { actor.run().await });

        let actor_drop_handle = AbortOnDropHandle::new(handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        Ok(MeshStatus {
            actor_drop_handle,
            actor_tx,
        })
    }

    pub async fn get_all(&self) -> Result<Vec<PeerState>> {
        let (tx, rx) = oneshot::channel();
        self.actor_tx
            .send(ToNodeActor::GetAll { reply: tx })
            .await?;
        rx.await?
    }

    pub async fn update(&self, updates: Vec<PeerState>) -> Result<()> {
        self.actor_tx
            .send(ToNodeActor::MeshPeerUpdates(updates))
            .await?;
        Ok(())
    }
}
