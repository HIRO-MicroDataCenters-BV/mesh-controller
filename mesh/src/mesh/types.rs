use meshresource::{meshpeer::PeerStatus, types::PeerState};
use serde::{Deserialize, Serialize};

use crate::network::discovery::{
    nodes::{MembershipState, PeerEvent},
    types::{MembershipUpdate, PeerStateUpdate},
};

impl From<MembershipState> for PeerStatus {
    fn from(val: MembershipState) -> Self {
        match val {
            MembershipState::Ready { .. } => PeerStatus::Ready,
            MembershipState::NotReady { .. } => PeerStatus::NotReady,
            MembershipState::Unavailable { .. } => PeerStatus::Unavailable,
            MembershipState::Unknown { .. } => PeerStatus::Unknown,
        }
    }
}

impl From<PeerStateUpdate> for PeerState {
    fn from(val: PeerStateUpdate) -> Self {
        PeerState {
            peer_id: val.peer.to_hex(),
            state: val.state.into(),
            state_since: val.state.get_since(),
            instance: val.instance.map(|i| meshresource::types::InstanceId {
                zone: i.zone,
                zone_start_time: i.start_time,
            }),
            update_timestamp: val.timestamp,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MembershipEvent {
    Update {
        update: MembershipUpdate,
        peer_event: PeerEvent,
    },
}

impl MembershipEvent {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("encoding network message");
        bytes
    }
}

impl TryFrom<Vec<u8>> for MembershipEvent {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        ciborium::from_reader(&bytes[..])
    }
}
