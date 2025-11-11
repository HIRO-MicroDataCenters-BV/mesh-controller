use crate::{
    meshpeer::{
        IntoTimeExt, MeshPeer, MeshPeerInstance, MeshPeerStatus, MeshPeerStatusCondition,
        PeerStatus,
    },
    types::PeerState,
};
use k8s_openapi::api::core::v1::Event;
use k8s_openapi::api::core::v1::ObjectReference;
use meshkube::event::{EventType, create_event};
use std::env;

pub fn update(peer: &mut MeshPeer, peer_state: &PeerState) {
    peer.spec.identity.public_key = peer_state.peer_id.to_owned();
    let status = peer.status.get_or_insert_default();
    update_status(status, peer_state);
}

fn update_status(status: &mut MeshPeerStatus, peer_state: &PeerState) {
    let instance = peer_state.instance.as_ref().map(|i| MeshPeerInstance {
        zone: i.zone.to_owned(),
        start_time: i.zone_start_time.to_time(),
        start_timestamp: i.zone_start_time,
    });

    status.instance = instance;
    status.update_time = peer_state.update_timestamp.to_time();
    status.status = peer_state.state;

    update_conditions(status, peer_state);
}

fn update_conditions(status: &mut MeshPeerStatus, incoming: &PeerState) {
    match &incoming.state {
        PeerStatus::Ready => {
            let ready =
                MeshPeerStatusCondition::new(incoming.state.to_string(), incoming.state_since);
            status.add_or_update(ready);
            status.set_condition_if(&["NotReady"], "False", incoming.update_timestamp, "True");
            status.remove_condition("Unavailable");
            status.remove_condition("Unknown");
        }
        PeerStatus::NotReady => {
            let not_ready =
                MeshPeerStatusCondition::new(incoming.state.to_string(), incoming.state_since);
            status.add_or_update(not_ready);
            status.set_condition_if(&["Ready"], "True", incoming.update_timestamp, "False");
            status.remove_condition("Unavailable");
            status.remove_condition("Unknown");
        }
        PeerStatus::Unavailable => {
            let unavailable =
                MeshPeerStatusCondition::new(incoming.state.to_string(), incoming.state_since);
            status.add_or_update(unavailable);
            status.set_condition_if(&["Ready"], "True", incoming.update_timestamp, "False");
            status.remove_condition("NotReady");
            status.remove_condition("Unknown");
        }
        PeerStatus::Unknown => {
            let unknown =
                MeshPeerStatusCondition::new(incoming.state.to_string(), incoming.state_since);
            status.add_or_update(unknown);
            status.remove_condition("NotReady");
            status.remove_condition("Ready");
            status.remove_condition("Unavailable");
        }
    }
}

pub fn create_mesh_event(object_ref: ObjectReference, update: &PeerState) -> Event {
    let (message, event_type) = match update.state {
        PeerStatus::Ready => ("Peer is ready".into(), EventType::Normal),
        PeerStatus::NotReady => ("Peer is not ready".into(), EventType::Warning),
        PeerStatus::Unavailable => ("Peer is unavailable".into(), EventType::Warning),
        PeerStatus::Unknown => ("Peer state is unknown".into(), EventType::Warning),
    };
    let reporting_instance = env::var("POD_NAME").unwrap_or_else(|_| "mesh_controller".to_string());
    create_event(
        object_ref,
        event_type,
        Some("PeerStateChange".into()),
        Some(message),
        "dcp.hiro.io/mesh-controller".into(),
        reporting_instance,
        "Processing".into(),
        update.update_timestamp,
    )
}
