use std::collections::HashMap;

use crate::network::discovery::types::{Membership, Timestamp};

pub struct PeerState {
    state: MembershipState,
}

impl PeerState {
    pub fn new() -> PeerState {
        PeerState {
            state: MembershipState::Unknown { since: 0 },
        }
    }

    pub fn on_event(&self, event: PeerEvent) -> Option<PeerState> {
        None
    }
}

pub enum PeerEvent {
    PeerUp(Timestamp),
    PeerDown(Timestamp),
    Tick(Timestamp),
}

#[derive(Clone, Debug)]
pub enum MembershipState {
    Ready { since: Timestamp },
    NotReady { since: Timestamp },
    Unknown { since: Timestamp },
}

pub struct Peers {
    peers: HashMap<String, PeerState>,
}

impl Peers {
    pub fn new() -> Peers {
        Peers {
            peers: HashMap::new(),
        }
    }

    pub fn on_event(&self, event: PeerEvent) -> Option<Membership> {
        None
    }
}
