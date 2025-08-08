use std::time::Duration;

use dashmap::DashMap;
use futures::{
    FutureExt, StreamExt, TryFutureExt,
    future::{MapErr, Shared},
    stream::SelectAll,
};
use iroh_base::{NodeAddr, PublicKey};
use p2panda_discovery::{BoxedStream, Discovery, DiscoveryEvent};
use p2panda_net::SystemEvent;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::trace;

use crate::{
    JoinErrToStr,
    mesh::topic::{MeshTopic, MeshTopicLogMap},
    network::discovery::{event::MembershipEvent, peers::Peers},
};
use anyhow::Result;

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(1);

pub enum ToMembershipDiscoveryActor {
    Subscribe {
        reply: oneshot::Sender<broadcast::Receiver<MembershipEvent>>,
    },
    ConsumeSystemEvents {
        events: broadcast::Receiver<SystemEvent<MeshTopic>>,
    },
}

#[derive(Debug, Clone)]
pub struct MembershipDiscovery {
    #[allow(dead_code)]
    handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
    actor_tx: mpsc::Sender<ToMembershipDiscoveryActor>,
}

impl MembershipDiscovery {
    pub fn new(
        topic_log_map: MeshTopicLogMap,
        cancelation: CancellationToken,
    ) -> MembershipDiscovery {
        let (actor_tx, inbox) = mpsc::channel::<ToMembershipDiscoveryActor>(512);
        let mut inner = MembershipDiscoveryActor::new(topic_log_map, inbox, cancelation);
        let handle = tokio::spawn(async move {
            inner.run().await;
        });

        let handle = AbortOnDropHandle::new(handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        MembershipDiscovery { handle, actor_tx }
    }

    pub async fn subscribe_events(&self) -> Result<broadcast::Receiver<MembershipEvent>> {
        let (reply, reply_rx) = oneshot::channel();
        self.actor_tx
            .send(ToMembershipDiscoveryActor::Subscribe { reply })
            .await?;
        Ok(reply_rx.await?)
    }

    pub async fn consume_system_events(
        &self,
        events: broadcast::Receiver<SystemEvent<MeshTopic>>,
    ) -> Result<()> {
        self.actor_tx
            .send(ToMembershipDiscoveryActor::ConsumeSystemEvents { events })
            .await?;
        Ok(())
    }
}

pub struct MembershipDiscoveryActor {
    peers: Peers,
    topic_log_map: MeshTopicLogMap,
    cancelation: CancellationToken,
    inbox: mpsc::Receiver<ToMembershipDiscoveryActor>,
    system_events_rx: SelectAll<BroadcastStream<SystemEvent<MeshTopic>>>,
    membership_events: Option<tokio::sync::broadcast::Sender<MembershipEvent>>,
}

impl MembershipDiscoveryActor {
    fn new(
        topic_log_map: MeshTopicLogMap,
        inbox: mpsc::Receiver<ToMembershipDiscoveryActor>,
        cancelation: CancellationToken,
    ) -> MembershipDiscoveryActor {
        MembershipDiscoveryActor {
            peers: Peers::new(),
            topic_log_map,
            cancelation,
            inbox,
            system_events_rx: SelectAll::new(),
            membership_events: None,
        }
    }

    async fn run(&mut self) {
        let mut state_update_interval = tokio::time::interval(DEFAULT_TICK_INTERVAL);

        loop {
            tokio::select! {
                biased;
                Some(message) = self.inbox.recv() => self.on_actor_message(message),
                Some(Ok(event)) = self.system_events_rx.next() => self.on_system_event(event),
                _ = state_update_interval.tick() => self.on_state_update_tick(),
                _ = self.cancelation.cancelled() => break,
            }
        }
    }

    fn on_actor_message(&mut self, message: ToMembershipDiscoveryActor) {
        match message {
            ToMembershipDiscoveryActor::Subscribe { reply } => self.on_subscribe_events(reply),
            ToMembershipDiscoveryActor::ConsumeSystemEvents { events } => {
                self.on_consume_events(events)
            }
        }
    }

    fn on_subscribe_events(
        &mut self,
        reply: oneshot::Sender<broadcast::Receiver<MembershipEvent>>,
    ) {
        let events_rx = self.events();
        reply.send(events_rx).ok();
    }

    fn on_consume_events(&mut self, events: broadcast::Receiver<SystemEvent<MeshTopic>>) {
        let stream = BroadcastStream::new(events);
        self.system_events_rx.push(stream);
    }

    fn events(&mut self) -> tokio::sync::broadcast::Receiver<MembershipEvent> {
        if let Some(events_tx) = &self.membership_events {
            events_tx.subscribe()
        } else {
            let (events_tx, events_rx) = tokio::sync::broadcast::channel(128);
            self.membership_events = Some(events_tx);
            events_rx
        }
    }

    fn on_state_update_tick(&mut self) {
        // self.peers.tick(self.clock.now_millis())
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
        // self.peers.on_peer_down(&peer);
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

impl std::fmt::Debug for MembershipDiscoveryActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MembershipDiscovery")
            .field("topic_log_map", &self.topic_log_map)
            .finish()
    }
}
