use std::time::Duration;

use dashmap::DashMap;
use futures::{
    FutureExt, TryFutureExt,
    future::{MapErr, Shared},
};
use iroh_base::{NodeAddr, PublicKey};
use p2panda_discovery::{BoxedStream, Discovery, DiscoveryEvent};
use p2panda_net::SystemEvent;
use tokio::task::JoinError;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::trace;

use crate::{
    JoinErrToStr,
    config::configuration::{DiscoveryOptions, KnownNode},
    mesh::topic::{MeshTopic, MeshTopicLogMap},
    network::discovery::static_lookup::KnownPeers,
};
use anyhow::Result;
use tokio::sync::broadcast::Receiver;

type DiscoveryReceiver = loole::Receiver<Result<DiscoveryEvent>>;
type DiscoverySender = loole::Sender<Result<DiscoveryEvent>>;

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct MembershipDiscovery {
    #[allow(dead_code)]
    handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
    discovery_rx: DiscoveryReceiver,
}

impl MembershipDiscovery {
    pub fn new(
        topic_log_map: MeshTopicLogMap,
        known_nodes: &Vec<KnownNode>,
        system_events: Receiver<SystemEvent<MeshTopic>>,
        cancelation: CancellationToken,
        options: DiscoveryOptions,
    ) -> MembershipDiscovery {
        let (discovery_tx, discovery_rx) = loole::bounded(64);
        let mut inner = MembershipDiscoveryInner::new(
            known_nodes,
            options,
            topic_log_map,
            system_events,
            cancelation,
            discovery_tx,
        );
        let handle = tokio::spawn(async move {
            inner.run().await;
        });

        let handle = AbortOnDropHandle::new(handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        MembershipDiscovery {
            handle,
            discovery_rx,
        }
    }
}

impl Discovery for MembershipDiscovery {
    fn update_local_address(&self, _node_addr: &NodeAddr) -> Result<()> {
        Ok(())
    }

    fn subscribe(&self, _network_id: [u8; 32]) -> Option<BoxedStream<Result<DiscoveryEvent>>> {
        Some(Box::pin(self.discovery_rx.clone().into_stream()))
    }
}

pub struct MembershipDiscoveryInner {
    peers: DashMap<String, PeerState>,
    static_peers: KnownPeers,
    topic_log_map: MeshTopicLogMap,
    system_events: Receiver<SystemEvent<MeshTopic>>,
    cancelation: CancellationToken,
    options: DiscoveryOptions,
    discovery_tx: DiscoverySender,
}

impl MembershipDiscoveryInner {
    fn new(
        known_nodes: &Vec<KnownNode>,
        options: DiscoveryOptions,
        topic_log_map: MeshTopicLogMap,
        system_events: Receiver<SystemEvent<MeshTopic>>,
        cancelation: CancellationToken,
        discovery_tx: DiscoverySender,
    ) -> MembershipDiscoveryInner {
        MembershipDiscoveryInner {
            static_peers: KnownPeers::new(known_nodes),
            peers: DashMap::new(),
            topic_log_map,
            system_events,
            cancelation,
            options,
            discovery_tx,
        }
    }

    async fn run(&mut self) {
        let mut state_update_interval = tokio::time::interval(DEFAULT_TICK_INTERVAL);

        let static_discovery_duration = Duration::from_secs(self.options.query_interval_seconds);
        let mut static_discovery_interval = tokio::time::interval(static_discovery_duration);

        loop {
            tokio::select! {
                biased;
                Ok(event) = self.system_events.recv() => self.on_system_event(event),
                _ = state_update_interval.tick() => self.on_state_update_tick(),
                _ = static_discovery_interval.tick() => self.on_discovery_tick().await,
                _ = self.cancelation.cancelled() => break,
            }
        }
    }

    fn on_state_update_tick(&mut self) {}

    async fn on_discovery_tick(&mut self) {
        let discovered_peers = self.static_peers.discover_and_update().await;
        if !discovered_peers.is_empty() {
            trace!("Peer discovery result: {:?}", discovered_peers);
        }

        for node_addr in discovered_peers {
            self.discovery_tx
                .send(Ok(DiscoveryEvent {
                    provenance: "peer_discovery",
                    node_addr,
                }))
                .ok();
        }
    }

    fn on_system_event(&mut self, event: SystemEvent<MeshTopic>) {
        match event {
            SystemEvent::GossipNeighborDown { peer, .. } => {
                tracing::info!("NeighborDown: {}", peer.to_hex());
                self.on_peer_down(peer)
            }
            SystemEvent::GossipNeighborUp { peer, .. } => {
                tracing::info!("NeighborUp: {}", peer.to_hex());
                self.on_peer_up(peer);
            }
            SystemEvent::PeerDiscovered { peer } => {
                self.on_peer_discovered(peer);
            }
            SystemEvent::SyncStarted { peer, .. } => {
                tracing::trace!("sync started: {}", peer.to_hex());
            }
            SystemEvent::SyncDone { peer, .. } => {
                tracing::trace!("sync done: {}", peer.to_hex());
            }
            SystemEvent::SyncFailed { peer, .. } => {
                tracing::trace!("sync failed: {}", peer.to_hex());
            }
            _ => {}
        }
    }

    fn on_peer_down(&self, peer: p2panda_core::PublicKey) {
        self.topic_log_map.remove_peer(&peer);
    }
    fn on_peer_up(&self, peer: p2panda_core::PublicKey) {}
    fn on_peer_discovered(&self, peer: p2panda_core::PublicKey) {
        if !self.topic_log_map.has_peer(&peer) {
            tracing::info!("PeerDiscovered: {}", peer.to_hex());
            self.topic_log_map.add_peer(peer);
        }
    }
}

impl std::fmt::Debug for MembershipDiscoveryInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MembershipDiscovery")
            .field("peers-count", &self.peers.len())
            .field("topic_log_map", &self.topic_log_map)
            .finish()
    }
}

pub type Timestamp = u64;

struct PeerState {
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
}

#[derive(Clone, Debug)]
pub enum MembershipState {
    Ready { since: Timestamp },
    NotReady { since: Timestamp },
    Unknown { since: Timestamp },
}
