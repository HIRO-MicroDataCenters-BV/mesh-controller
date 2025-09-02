use std::collections::HashMap;

use stackable_operator::kube::api::ObjectMeta;

use crate::{
    meshpeer::{MeshPeer, MeshPeerSpec, MeshPeerStatus, PeerIdentity, PeerStatus},
    types::PeerUpdate,
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

    pub fn update_and_get(&mut self, update: PeerUpdate) -> &MeshPeer {
        let peer = self
            .peers
            .entry(update.peer_id.to_owned())
            .or_insert_with(|| Peers::create_peer(&update.peer_id));
        Peers::update(peer, &update);
        peer
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

    fn update(_peer: &mut MeshPeer, _update: &PeerUpdate) {}
}
