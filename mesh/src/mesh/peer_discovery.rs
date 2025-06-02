use futures::{
    FutureExt, TryFutureExt,
    future::{MapErr, Shared},
};
use p2panda_net::SystemEvent;
use tokio::{sync::broadcast::Receiver, task::JoinError};
use tokio_util::task::AbortOnDropHandle;

use crate::JoinErrToStr;

use super::topic::{MeshTopic, MeshTopicLogMap};

pub struct PeerDiscovery {
    #[allow(dead_code)]
    handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl PeerDiscovery {
    pub fn start(
        mut system_events: Receiver<SystemEvent<MeshTopic>>,
        topic_map: MeshTopicLogMap,
    ) -> Self {
        let handle = tokio::spawn(async move {
            while let Ok(event) = system_events.recv().await {
                match event {
                    SystemEvent::GossipNeighborDown { peer, .. } => {
                        tracing::info!("NeighborDown: {}", peer.to_hex());
                        topic_map.remove_peer(&peer);
                    }
                    SystemEvent::GossipNeighborUp { peer, .. } => {
                        tracing::info!("NeighborUp: {}", peer.to_hex());
                    }
                    SystemEvent::PeerDiscovered { peer } => {
                        if !topic_map.has_peer(&peer) {
                            tracing::info!("PeerDiscovered: {}", peer.to_hex());
                            topic_map.add_peer(peer);
                        }
                    }
                    SystemEvent::SyncStarted { peer, .. } => {
                        tracing::trace!("sync started: {}", peer.to_hex());
                    }
                    SystemEvent::SyncDone { peer, .. } => {
                        tracing::trace!("sync done: {}", peer.to_hex());
                    }
                    SystemEvent::SyncFailed { peer, .. } => {
                        tracing::trace!("sync failed: {}", peer.to_hex());
                    }
                    _ => {}
                }
            }
        });
        let handle = AbortOnDropHandle::new(handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        PeerDiscovery { handle }
    }
}
