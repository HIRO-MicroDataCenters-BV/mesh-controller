use anyhow::Result;
use k8s_openapi::api::core::v1::ObjectReference;

use crate::{
    meshpeer::{MeshPeer, MeshPeerExt, MeshPeerSpec, PeerIdentity},
    peers::{create_mesh_event, update},
    types::PeerState,
};
use anyhow::anyhow;
use kube::api::GroupVersionKind;
use kube::{Resource, api::ObjectMeta};
use meshkube::{
    client::{ClientError, KubeClient},
    kube::types::NamespacedName,
};
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
    gvk: GroupVersionKind,
    namespace: String,
}

impl MeshPeerResourceActor {
    pub async fn new(
        inbox: mpsc::Receiver<ToNodeActor>,
        client: KubeClient,
        cancelation: CancellationToken,
    ) -> Result<MeshPeerResourceActor> {
        let namespace = "default".into();
        let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "MeshPeer");
        Ok(MeshPeerResourceActor {
            namespace,
            gvk,
            client,
            inbox,
            cancelation,
        })
    }

    pub async fn load_from_store(
        client: KubeClient,
        gvk: &GroupVersionKind,
        namespace: &str,
    ) -> Result<Vec<MeshPeer>> {
        let items = client.list(gvk, &Some(namespace.into())).await?;
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
            ToNodeActor::GetAll { reply } => self.on_get_all(reply).await,
        }
    }

    async fn on_peer_update(&mut self, incoming: PeerState) -> Result<()> {
        let object_ref = self.try_update(&incoming).await?;
        self.emit_event(object_ref, &incoming).await?;
        Ok(())
    }

    async fn try_update(&self, incoming: &PeerState) -> Result<ObjectReference, ClientError> {
        let mut attempts = 10;
        let mut result = Err(ClientError::VersionConflict);
        while attempts > 0 {
            result = self.update_resource(incoming).await;
            match &result {
                Err(ClientError::VersionConflict) => (),
                Ok(_) | Err(_) => return result,
            }
            attempts -= 1;
        }
        result
    }

    async fn update_resource(&self, incoming: &PeerState) -> Result<ObjectReference, ClientError> {
        let name = NamespacedName::new(self.namespace.to_owned(), incoming.peer_id.to_owned());
        let maybe_peer = self.client.get(&self.gvk, &name).await?;
        let mut mesh_peer: MeshPeer = if let Some(peer) = maybe_peer {
            peer.try_parse().map_err(ClientError::ResourceFormatError)?
        } else {
            Self::create_peer(&incoming.peer_id, &name.namespace)
        };

        update(&mut mesh_peer, incoming);
        let object_ref = mesh_peer.object_ref(&());

        let object = mesh_peer.to_owned().to_object()?;
        self.client.patch_apply(object).await?;

        Ok(object_ref)
    }

    async fn emit_event(&self, object_ref: ObjectReference, update: &PeerState) -> Result<()> {
        let event = create_mesh_event(object_ref, update);
        self.client.emit_event(event).await
    }

    async fn on_peer_updates(&mut self, updates: Vec<PeerState>) -> Result<()> {
        for update in updates {
            self.on_peer_update(update).await?;
        }
        Ok(())
    }

    async fn on_get_all(&self, reply: oneshot::Sender<Result<Vec<PeerState>>>) -> Result<()> {
        let peers = Self::load_from_store(self.client.clone(), &self.gvk, &self.namespace)
            .await
            .map(|p| p.iter().map(|p| p.into()).collect());
        reply.send(peers).ok();
        Ok(())
    }

    fn create_peer(peer_id: &str, namespace: &str) -> MeshPeer {
        MeshPeer {
            metadata: ObjectMeta {
                name: Some(peer_id.into()),
                namespace: Some(namespace.into()),
                ..Default::default()
            },
            spec: MeshPeerSpec {
                identity: PeerIdentity {
                    public_key: peer_id.into(),
                    endpoints: vec![],
                },
            },
            status: None,
        }
    }
}
