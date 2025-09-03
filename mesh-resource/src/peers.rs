use kube::api::ObjectMeta;
use std::collections::HashMap;

use crate::{
    meshpeer::{
        MeshPeer, MeshPeerInstance, MeshPeerSpec, MeshPeerStatus, MeshPeerStatusCondition,
        PeerIdentity, PeerStatus,
    },
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

    pub fn get_all(&self) -> Vec<PeerState> {
        self.peers.values().map(PeerState::from).collect()
    }

    pub fn update_and_get(&mut self, update: PeerState) -> &MeshPeer {
        let peer = self
            .peers
            .entry(update.peer_id.to_owned())
            .or_insert_with(|| Peers::create_peer(&update.peer_id));
        Peers::update(peer, &update);
        peer
    }

    fn create_peer(peer_id: &str) -> MeshPeer {
        MeshPeer {
            metadata: ObjectMeta {
                name: Some(peer_id.into()),
                namespace: Some("default".into()),
                ..Default::default()
            },
            spec: MeshPeerSpec {
                identity: PeerIdentity {
                    public_key: peer_id.into(),
                    endpoints: vec![],
                },
            },
            status: Some(MeshPeerStatus {
                status: PeerStatus::Unavailable,
                instance: None,
                update_time: 0,
                conditions: vec![],
            }),
        }
    }

    fn update(peer: &mut MeshPeer, peer_state: &PeerState) {
        peer.spec.identity.public_key = peer_state.peer_id.to_owned();
        let mut status = Self::to_peer_status(peer_state);
        Self::update_conditions(&mut status, peer_state);
        peer.status = Some(status);
    }

    fn to_peer_status(peer_state: &PeerState) -> MeshPeerStatus {
        let instance = peer_state.instance.as_ref().map(|i| MeshPeerInstance {
            zone: i.zone.to_owned(),
            start_time: i.zone_start_time,
        });
        MeshPeerStatus {
            conditions: vec![],
            instance,
            update_time: peer_state.update_timestamp,
            status: peer_state.state,
        }
    }

    fn update_conditions(status: &mut MeshPeerStatus, incoming: &PeerState) {
        match &incoming.state {
            PeerStatus::Ready => {
                let ready =
                    MeshPeerStatusCondition::new(incoming.state.to_string(), incoming.state_since);
                status.add_or_update(ready);
                status.set_condition_if(&["NotReady"], "False", incoming.update_timestamp, "True");
                status.remove_condition("Unavailable");
            }
            PeerStatus::NotReady => {
                let not_ready =
                    MeshPeerStatusCondition::new(incoming.state.to_string(), incoming.state_since);
                status.add_or_update(not_ready);
                status.set_condition_if(&["Ready"], "True", incoming.update_timestamp, "False");
                status.remove_condition("Unavailable");
            }
            PeerStatus::Unavailable => {
                let unavailable =
                    MeshPeerStatusCondition::new(incoming.state.to_string(), incoming.state_since);
                status.add_or_update(unavailable);
                status.set_condition_if(&["Ready"], "True", incoming.update_timestamp, "False");
                status.remove_condition("NotReady");
            }
        }
    }
}
