use async_trait::async_trait;
use dashmap::DashMap;
use p2panda_core::PublicKey;
use p2panda_sync::log_sync::TopicLogMap;
use std::cmp::Ordering;
use std::{collections::HashMap, sync::Arc};

use crate::mesh::topic::{Logs, MeshLogId, MeshTopic};
use crate::network::discovery::types::{Membership, Timestamp};
use std::time::Duration;

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

    pub fn on_event(&self, event: PeerEvent) -> Option<Membership> {
        let now = event.timestamp();
        let updated = match &event {
            PeerEvent::PeerDiscovered { peer, .. } => {
                let mut entry = self
                    .inner
                    .peers
                    .entry(*peer)
                    .or_insert_with(|| PeerState::new(self.timeout));
                entry.value_mut().on_event(event)
            }
            PeerEvent::PeerUp { peer, .. } => {
                let mut entry = self
                    .inner
                    .peers
                    .entry(*peer)
                    .or_insert_with(|| PeerState::new(self.timeout));
                entry.value_mut().on_event(event)
            }
            PeerEvent::PeerDown { peer, .. } => {
                let mut entry = self
                    .inner
                    .peers
                    .entry(*peer)
                    .or_insert_with(|| PeerState::new(self.timeout));
                entry.value_mut().on_event(event)
            }
            PeerEvent::Tick { .. } => self
                .inner
                .peers
                .iter_mut()
                .map(|mut entry| entry.value_mut().on_event(event))
                .reduce(|left, right| left | right)
                .unwrap_or(false),
        };
        if updated {
            Some(self.get_membership(now))
        } else {
            None
        }
    }

    pub fn get_membership(&self, now: Timestamp) -> Membership {
        let mut membership = Membership::new(now);
        self.inner
            .peers
            .iter()
            .flat_map(|entry| match entry.state {
                MembershipState::Ready { .. } | MembershipState::NotReady { .. } => {
                    entry.log_id.as_ref().map(|v| v.0.clone())
                }
                _ => None,
            })
            .for_each(|instance| membership.add(instance));
        membership.add(self.inner.log_id.0.to_owned());
        membership
    }

    pub fn get_latest_log(&self, peer: &PublicKey) -> Option<MeshLogId> {
        if peer == &self.inner.owner {
            // If the peer is the owner, return the log_id of the owner.
            Some(self.inner.log_id.clone())
        } else {
            self.inner
                .peers
                .get(peer)
                .map(|e| e.value().log_id.to_owned())
                .unwrap_or(None)
        }
    }

    pub fn update_log(&self, peer: PublicKey, log_id: MeshLogId, now: Timestamp) {
        if peer == self.inner.owner {
            // If the peer is the owner, log update is handled via restart of application.
            return;
        }

        let mut entry = self
            .inner
            .peers
            .entry(peer)
            .or_insert_with(|| PeerState::new(self.timeout));
        let previous_log_id = entry
            .value_mut()
            .update_log_id(log_id, PeerEvent::PeerUp { peer, now });
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

    pub fn get_peer_logs(&self) -> HashMap<PublicKey, MeshLogId> {
        self.inner
            .peers
            .iter()
            .filter_map(|entry| {
                let peer = entry.key().to_owned();
                entry
                    .value()
                    .log_id
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
                .log_id
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
    log_id: Option<MeshLogId>,
    state: MembershipState,
    timeout: Duration,
}

impl PeerState {
    pub fn new(timeout: Duration) -> PeerState {
        PeerState {
            state: MembershipState::Unavailable { since: 0 },
            log_id: None,
            timeout,
        }
    }

    pub fn on_event(&mut self, event: PeerEvent) -> bool {
        let new_state = match (event, &self.state) {
            (
                PeerEvent::Tick { .. },
                MembershipState::Ready { .. } | MembershipState::Unavailable { .. },
            ) => None,
            (PeerEvent::Tick { now }, MembershipState::NotReady { since }) => {
                if *since < now.saturating_sub(self.timeout.as_millis() as u64) {
                    tracing::info!("Peer Unavailable");
                    Some(MembershipState::Unavailable { since: now })
                } else {
                    None
                }
            }

            (
                PeerEvent::PeerDiscovered { now, peer },
                MembershipState::Unavailable { .. } | MembershipState::NotReady { .. },
            ) => {
                tracing::info!("Peer Discovered {:?}", peer.to_hex());

                Some(MembershipState::Ready { since: now })
            }
            (PeerEvent::PeerDiscovered { .. }, MembershipState::Ready { .. }) => None,

            (
                PeerEvent::PeerUp { now, .. },
                MembershipState::Unavailable { .. } | MembershipState::NotReady { .. },
            ) => {
                tracing::info!("Peer Ready");
                Some(MembershipState::Ready { since: now })
            }
            (PeerEvent::PeerUp { .. }, MembershipState::Ready { .. }) => None,

            (
                PeerEvent::PeerDown { .. },
                MembershipState::Unavailable { .. } | MembershipState::NotReady { .. },
            ) => None,
            (PeerEvent::PeerDown { now, .. }, MembershipState::Ready { .. }) => {
                tracing::info!("Peer NotReady");
                Some(MembershipState::NotReady { since: now })
            }
        };
        if new_state.is_some() {
            tracing::info!("new state {new_state:?}");
            self.state = new_state.unwrap();
            true
        } else {
            false
        }
    }

    pub fn update_log_id(&mut self, log_id: MeshLogId, event: PeerEvent) -> Option<MeshLogId> {
        let simulate_peer_up = match &self.log_id {
            Some(existing) => existing.0.start_time.cmp(&log_id.0.start_time) == Ordering::Greater,
            None => true,
        };
        if simulate_peer_up {
            self.on_event(event);
        }
        self.log_id.replace(log_id)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum PeerEvent {
    PeerDiscovered { peer: PublicKey, now: Timestamp },
    PeerUp { peer: PublicKey, now: Timestamp },
    PeerDown { peer: PublicKey, now: Timestamp },
    Tick { now: Timestamp },
}

impl PeerEvent {
    pub fn timestamp(&self) -> Timestamp {
        match self {
            PeerEvent::Tick { now } => *now,
            PeerEvent::PeerDiscovered { now, .. } => *now,
            PeerEvent::PeerUp { now, .. } => *now,
            PeerEvent::PeerDown { now, .. } => *now,
        }
    }
}

#[derive(Clone, Debug)]
pub enum MembershipState {
    Ready { since: Timestamp },
    NotReady { since: Timestamp },
    Unavailable { since: Timestamp },
}
