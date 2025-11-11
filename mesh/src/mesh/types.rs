use meshresource::{meshpeer::PeerStatus, types::PeerState};

use crate::network::discovery::{nodes::MembershipState, types::PeerStateUpdate};

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
