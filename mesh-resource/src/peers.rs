use kube::api::ObjectMeta;
use std::collections::HashMap;

use crate::{
    meshpeer::{MeshPeer, MeshPeerSpec, MeshPeerStatus, PeerIdentity, PeerStatus},
    types::PeerState,
};

pub struct Peers {
    peers: HashMap<String, MeshPeer>,
}

impl Default for Peers {
    fn default() -> Self {
        Self::new()
    }
}

impl Peers {
    pub fn new() -> Peers {
        Peers {
            peers: HashMap::new(),
        }
    }

    pub fn from(peers: Vec<MeshPeer>) -> Peers {
        let peers = peers
            .into_iter()
            .map(|p| (p.spec.identity.public_key.clone(), p))
            .collect();
        Peers { peers }
    }

    pub fn update_and_get(&mut self, update: PeerState) -> &MeshPeer {
        let peer = self
            .peers
            .entry(update.peer_id.to_owned())
            .or_insert_with(|| Peers::create_peer(&update.peer_id));
        Peers::update(peer, &update);
        peer
    }

    pub fn get_all(&self) -> Vec<PeerState> {
        // self.peers.iter().map(|(_,p)|PeerState::from(p)).collect()
        todo!()
    }

    fn create_peer(peer_id: &str) -> MeshPeer {
        MeshPeer {
            metadata: ObjectMeta::default(),
            spec: MeshPeerSpec {
                identity: PeerIdentity {
                    public_key: peer_id.into(),
                    endpoints: vec![],
                },
            },
            status: Some(MeshPeerStatus {
                status: PeerStatus::Unavailable,
                conditions: vec![],
            }),
        }
    }

    fn update(_peer: &mut MeshPeer, _update: &PeerState) {}
}
