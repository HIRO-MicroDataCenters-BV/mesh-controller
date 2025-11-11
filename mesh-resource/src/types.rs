use crate::meshpeer::{MeshPeer, PeerStatus};

#[derive(Debug, Clone)]
pub struct PeerState {
    pub peer_id: String,
    pub state: PeerStatus,
    pub state_since: u64,
    pub instance: Option<InstanceId>,
    pub update_timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct InstanceId {
    pub zone: String,
    pub zone_start_time: u64,
}

impl From<&MeshPeer> for PeerState {
    fn from(mesh_peer: &MeshPeer) -> Self {
        let status = mesh_peer.status.clone().unwrap_or_default();
        PeerState {
            peer_id: mesh_peer.spec.identity.public_key.to_owned(),
            state: status.status,
            state_since: status.update_time.0.timestamp_millis() as u64,
            instance: status.instance.as_ref().map(|i| InstanceId {
                zone: i.zone.to_owned(),
                zone_start_time: i.start_timestamp,
            }),
            update_timestamp: status.update_time.0.timestamp_millis() as u64,
        }
    }
}
