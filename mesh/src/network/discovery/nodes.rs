use crate::mesh::topic::{Logs, MeshLogId, MeshTopic};
use crate::network::discovery::types::{Membership, MembershipUpdate, PeerStateUpdate, Timestamp};
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use meshresource::meshpeer::PeerStatus;
use p2panda_core::PublicKey;
use p2panda_sync::log_sync::TopicLogMap;
use std::cmp::Ordering;
use std::str::FromStr;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tracing::{Span, info};

#[derive(Debug, Clone)]
pub struct Nodes {
    inner: Arc<NodesInner>,
    timeout: Duration,
}

impl Nodes {
    pub fn new(owner: PublicKey, log_id: MeshLogId, timeout: Duration) -> Nodes {
        Nodes {
            inner: Arc::new(NodesInner {
                owner,
                log_id,
                peers: DashMap::new(),
                obsolete_logs: DashMap::new(),
            }),
            timeout,
        }
    }

    pub fn load_initial_state(&self, peers: Vec<PublicKey>, now: Timestamp) {
        for peer in peers {
            let state = PeerState::from_previous_state(peer.to_owned(), self.timeout, now);
            self.inner.peers.insert(peer, state);
        }
    }

    pub fn on_event(&self, span: &Span, event: PeerEvent) -> Option<MembershipUpdate> {
        let now = event.timestamp();
        let updated_peers = if let Some(peer) = event.get_peer() {
            let mut entry = self
                .inner
                .peers
                .entry(peer)
                .or_insert_with(|| PeerState::new(peer.to_owned(), self.timeout));
            if entry.value_mut().on_event(span, event) {
                vec![peer.to_owned()]
            } else {
                Vec::new()
            }
        } else {
            self.inner
                .peers
                .iter_mut()
                .flat_map(|mut entry| {
                    if entry.value_mut().on_event(span, event) {
                        vec![entry.key().to_owned()]
                    } else {
                        Vec::new()
                    }
                })
                .collect()
        };
        if !updated_peers.is_empty() {
            let update = self.get_membership_update(now, &updated_peers);
            Some(update)
        } else {
            None
        }
    }

    pub fn get_membership_update(
        &self,
        now: Timestamp,
        updated_peers: &[PublicKey],
    ) -> MembershipUpdate {
        let membership = self.get_membership(now);
        let peers = self.get_peer_states(now, updated_peers);
        MembershipUpdate { membership, peers }
    }

    pub fn get_membership(&self, now: Timestamp) -> Membership {
        let mut membership = Membership::new(now);
        self.inner
            .peers
            .iter()
            .flat_map(|entry| match entry.state {
                MembershipState::Ready { .. } | MembershipState::NotReady { .. } => {
                    entry.active_log.as_ref().map(|v| v.0.clone())
                }
                _ => None,
            })
            .for_each(|instance| membership.add(instance));
        // Adding self
        membership.add(self.inner.log_id.0.to_owned());
        membership
    }

    /// Returns a vector of peers that are currently in a 'Ready' state.
    pub fn get_peer_states(
        &self,
        now: Timestamp,
        updated_peers: &[PublicKey],
    ) -> Vec<PeerStateUpdate> {
        let mut peer_states: Vec<PeerStateUpdate> = self
            .inner
            .peers
            .iter()
            .filter(|p| updated_peers.contains(&p.peer))
            .map(
                |p: dashmap::mapref::multiple::RefMulti<'_, PublicKey, PeerState>| {
                    let state = p.value();
                    PeerStateUpdate {
                        peer: state.peer.to_owned(),
                        state: state.state,
                        instance: state.active_log.as_ref().map(|log| log.0.clone()),
                        timestamp: now,
                    }
                },
            )
            .collect();

        if updated_peers.contains(&self.inner.owner) {
            // Adding self
            peer_states.push(PeerStateUpdate {
                peer: self.inner.owner,
                state: MembershipState::Ready {
                    since: self.inner.log_id.0.start_time,
                },
                instance: Some(self.inner.log_id.0.to_owned()),
                timestamp: now,
            });
        }
        peer_states
    }

    pub fn get_active_log(&self, peer: &PublicKey) -> Option<MeshLogId> {
        if peer == &self.inner.owner {
            // If the peer is the owner, return the log_id of the owner.
            Some(self.inner.log_id.clone())
        } else {
            self.inner
                .peers
                .get(peer)
                .map(|e| e.value().active_log.to_owned())
                .unwrap_or(None)
        }
    }

    pub fn update_log(&self, span: &Span, peer: PublicKey, log_id: MeshLogId, now: Timestamp) {
        if peer == self.inner.owner {
            // If the peer is the owner, log update is handled via restart of application.
            return;
        }

        let mut entry = self
            .inner
            .peers
            .entry(peer)
            .or_insert_with(|| PeerState::new(peer.to_owned(), self.timeout));
        let previous_log_id =
            entry
                .value_mut()
                .update_log_id(span, log_id, PeerEvent::PeerUp { peer, now });
        if let Some(log_id) = previous_log_id {
            self.inner
                .obsolete_logs
                .entry(peer)
                .or_default()
                .push(log_id);
        }
    }

    pub fn take_obsolete_log_ids(&self) -> Vec<(PublicKey, Vec<MeshLogId>)> {
        self.inner
            .obsolete_logs
            .iter()
            .map(|entry| entry.key().to_owned())
            .collect::<Vec<_>>()
            .into_iter()
            .filter_map(|k| self.inner.obsolete_logs.remove(&k))
            .collect()
    }

    pub fn get_remote_active_logs(&self) -> HashMap<PublicKey, MeshLogId> {
        self.inner
            .peers
            .iter()
            .filter_map(|entry| {
                let peer = entry.key().to_owned();
                entry
                    .value()
                    .active_log
                    .as_ref()
                    .map(|log_id| (peer, log_id.to_owned()))
            })
            .collect()
    }
}

#[async_trait]
impl TopicLogMap<MeshTopic, MeshLogId> for Nodes {
    async fn get(&self, _topic_query: &MeshTopic) -> Option<Logs<MeshLogId>> {
        let mut logs = Logs::new();
        self.inner.peers.iter().for_each(|peer| {
            let log_ids = peer
                .value()
                .active_log
                .as_ref()
                .map(|log_id| vec![log_id.clone()])
                .unwrap_or_default();
            logs.insert(peer.key().to_owned(), log_ids);
        });
        logs.insert(self.inner.owner.to_owned(), vec![self.inner.log_id.clone()]);
        Some(logs)
    }
}

#[derive(Debug)]
struct NodesInner {
    owner: PublicKey,
    log_id: MeshLogId,
    peers: DashMap<PublicKey, PeerState>,
    obsolete_logs: DashMap<PublicKey, Vec<MeshLogId>>,
}

#[derive(Clone, Debug)]
pub struct PeerState {
    pub peer: PublicKey,
    pub state: MembershipState,
    pub active_log: Option<MeshLogId>,
    timeout: Duration,
}

impl PeerState {
    pub fn new(peer: PublicKey, timeout: Duration) -> PeerState {
        PeerState {
            state: MembershipState::Unknown { since: 0 },
            active_log: None,
            peer,
            timeout,
        }
    }

    pub fn from_previous_state(peer: PublicKey, timeout: Duration, since: Timestamp) -> PeerState {
        PeerState {
            state: MembershipState::Unknown { since },
            active_log: None,
            peer,
            timeout,
        }
    }

    pub fn on_event(&mut self, span: &Span, event: PeerEvent) -> bool {
        let is_on_tick = matches!(event, PeerEvent::Tick { .. });

        let new_state = match (event, &self.state) {
            (
                PeerEvent::Tick { .. },
                MembershipState::Ready { .. } | MembershipState::Unavailable { .. },
            ) => None,
            (
                PeerEvent::Tick { now },
                MembershipState::NotReady { since } | MembershipState::Unknown { since },
            ) => {
                if *since < now.saturating_sub(self.timeout.as_millis() as u64) {
                    Some(MembershipState::Unavailable { since: now })
                } else {
                    None
                }
            }
            (
                PeerEvent::PeerDiscovered { now, .. },
                MembershipState::Unavailable { .. }
                | MembershipState::NotReady { .. }
                | MembershipState::Unknown { .. },
            ) => Some(MembershipState::Ready { since: now }),
            (PeerEvent::PeerDiscovered { .. }, MembershipState::Ready { .. }) => None,

            (
                PeerEvent::PeerUp { now, .. },
                MembershipState::Unavailable { .. }
                | MembershipState::NotReady { .. }
                | MembershipState::Unknown { .. },
            ) => Some(MembershipState::Ready { since: now }),
            (PeerEvent::PeerUp { .. }, MembershipState::Ready { .. }) => None,

            (
                PeerEvent::PeerDown { .. },
                MembershipState::Unavailable { .. }
                | MembershipState::NotReady { .. }
                | MembershipState::Unknown { .. },
            ) => None,
            (PeerEvent::PeerDown { now, .. }, MembershipState::Ready { .. }) => {
                Some(MembershipState::NotReady { since: now })
            }
            (
                PeerEvent::PeerUnknown { now, .. },
                MembershipState::Ready { .. }
                | MembershipState::Unavailable { .. }
                | MembershipState::NotReady { .. },
            ) => Some(MembershipState::Unknown { since: now }),
            (PeerEvent::PeerUnknown { .. }, MembershipState::Unknown { since, .. }) => {
                Some(MembershipState::Unknown { since: *since })
            }
        };
        if is_on_tick && new_state.is_some() {
            info!("new on tick state {:?}", new_state);
        }
        if let Some(new_state) = new_state {
            self.state = new_state;
            let active_log_id = self
                .active_log
                .as_ref()
                .map(|log| log.0.to_string())
                .unwrap_or("unknown".into());
            info!(parent: span, "{}({}): new state {}", self.peer.to_hex(), active_log_id, self.state);
            true
        } else {
            false
        }
    }

    pub fn update_log_id(
        &mut self,
        span: &Span,
        log_id: MeshLogId,
        event: PeerEvent,
    ) -> Option<MeshLogId> {
        let simulate_peer_up = match &self.active_log {
            Some(existing) => existing.0.start_time.cmp(&log_id.0.start_time) == Ordering::Greater,
            None => true,
        };
        if simulate_peer_up {
            self.on_event(span, event);
        }
        self.active_log.replace(log_id)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum PeerEvent {
    PeerUnknown { peer: PublicKey, now: Timestamp },
    PeerDiscovered { peer: PublicKey, now: Timestamp },
    PeerUp { peer: PublicKey, now: Timestamp },
    PeerDown { peer: PublicKey, now: Timestamp },
    Tick { now: Timestamp },
}

impl PeerEvent {
    pub fn timestamp(&self) -> Timestamp {
        match self {
            PeerEvent::Tick { now } => *now,
            PeerEvent::PeerUnknown { now, .. } => *now,
            PeerEvent::PeerDiscovered { now, .. } => *now,
            PeerEvent::PeerUp { now, .. } => *now,
            PeerEvent::PeerDown { now, .. } => *now,
        }
    }

    pub fn get_peer(&self) -> Option<PublicKey> {
        match self {
            PeerEvent::Tick { .. } => None,
            PeerEvent::PeerUnknown { peer, .. } => Some(*peer),
            PeerEvent::PeerDiscovered { peer, .. } => Some(*peer),
            PeerEvent::PeerUp { peer, .. } => Some(*peer),
            PeerEvent::PeerDown { peer, .. } => Some(*peer),
        }
    }

    pub fn discover_from(state: meshresource::types::PeerState) -> Result<PeerEvent> {
        let peer = PublicKey::from_str(&state.peer_id)?;
        let event = match state.state {
            PeerStatus::Ready => PeerEvent::PeerUp {
                peer,
                now: state.state_since,
            },
            PeerStatus::NotReady => PeerEvent::PeerDiscovered {
                peer,
                now: state.state_since,
            },
            PeerStatus::Unavailable => PeerEvent::PeerDown {
                peer,
                now: state.state_since,
            },
            PeerStatus::Unknown => PeerEvent::PeerUnknown {
                peer,
                now: state.state_since,
            },
        };
        Ok(event)
    }
}

#[derive(Clone, Debug, Copy)]
pub enum MembershipState {
    Ready { since: Timestamp },
    NotReady { since: Timestamp },
    Unavailable { since: Timestamp },
    Unknown { since: Timestamp },
}

impl MembershipState {
    pub fn get_since(&self) -> u64 {
        match self {
            MembershipState::Ready { since } => *since,
            MembershipState::NotReady { since } => *since,
            MembershipState::Unavailable { since } => *since,
            MembershipState::Unknown { since } => *since,
        }
    }

    pub fn from(state: meshresource::meshpeer::PeerStatus, since: Timestamp) -> Self {
        match state {
            meshresource::meshpeer::PeerStatus::Ready => MembershipState::Ready { since },
            meshresource::meshpeer::PeerStatus::NotReady => MembershipState::NotReady { since },
            meshresource::meshpeer::PeerStatus::Unavailable => {
                MembershipState::Unavailable { since }
            }
            meshresource::meshpeer::PeerStatus::Unknown => MembershipState::Unknown { since },
        }
    }
}

impl std::fmt::Display for MembershipState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MembershipState::Ready { since } => write!(f, "Ready(since = {since})"),
            MembershipState::NotReady { since } => write!(f, "NotReady(since = {since})"),
            MembershipState::Unavailable { since } => write!(f, "Unavailable(since = {since})"),
            MembershipState::Unknown { since } => write!(f, "Unknown(since = {since})"),
        }
    }
}
