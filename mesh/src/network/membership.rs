use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use iroh_base::{NodeAddr, NodeId};
use loole::Receiver;
use p2panda_core::PublicKey;
use p2panda_discovery::DiscoveryEvent;
use p2panda_discovery::{BoxedStream, Discovery};

use tokio_util::task::AbortOnDropHandle;

use crate::config::configuration::{DiscoveryOptions, KnownNode};
use crate::logs::topic::MeshTopicLogMap;

type SubscribeReceiver = Receiver<Result<DiscoveryEvent>>;

struct NodeInfo {
    pub addresses: Vec<String>,
    pub addr: Option<NodeAddr>,
}

struct KnownPeers {
    peers: HashMap<PublicKey, NodeInfo>,
}

impl KnownPeers {
    pub fn new(known_nodes: &Vec<KnownNode>) -> KnownPeers {
        let mut peers: HashMap<PublicKey, NodeInfo> = HashMap::new();
        for node in known_nodes {
            let node_info = NodeInfo {
                addr: None,
                addresses: node.direct_addresses.clone(),
            };
            peers.insert(node.public_key, node_info);
        }
        KnownPeers { peers }
    }

    pub async fn discover_and_update(&mut self) -> Vec<NodeAddr> {
        let mut discovered = vec![];
        for (public_key, info) in self.peers.iter_mut() {
            let mut direct_addresses = vec![];
            for fqdn in &info.addresses {
                let maybe_peers = tokio::net::lookup_host(fqdn).await;
                if let Ok(peers) = maybe_peers {
                    for resolved in peers {
                        direct_addresses.push(resolved);
                    }
                }
            }
            if direct_addresses.is_empty() {
                continue;
            }
            let key = NodeId::from_bytes(public_key.as_bytes()).expect("invalid public key");
            let node_addr = Some(NodeAddr::from_parts(key, None, direct_addresses));
            if node_addr != info.addr {
                info.addr = node_addr.clone();
                if let Some(addr) = node_addr {
                    discovered.push(addr);
                }
            }
        }
        discovered
    }
}

#[derive(Debug)]
pub struct Membership {
    #[allow(dead_code)]
    handle: AbortOnDropHandle<()>,
    rx: SubscribeReceiver,
}

impl Membership {
    pub fn new(
        known_nodes: &Vec<KnownNode>,
        options: DiscoveryOptions,
        topic_map: MeshTopicLogMap,
    ) -> Self {
        let mut peers = KnownPeers::new(known_nodes);
        let (sender, rx) = loole::bounded(64);

        let handle = tokio::task::spawn(async move {
            let query_interval = Duration::from_secs(options.query_interval_seconds);
            let mut interval = tokio::time::interval(query_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let discovery_result = peers.discover_and_update().await;
                        // trace!("Peer discovery result: {:?}", discovery_result);
                        for node_addr in discovery_result {
                            // TODO KT probably unverified public key. Use network events instead.
                            let key = PublicKey::from_bytes(node_addr.node_id.as_bytes())
                                .expect("public key already checked");
                            topic_map.add_peer(key);
                            sender.send(Ok(DiscoveryEvent{ provenance: "peer_discovery", node_addr })).ok();
                        }
                    },
                }
            }
        });

        Self {
            handle: AbortOnDropHandle::new(handle),
            rx,
        }
    }
}

impl Discovery for Membership {
    fn update_local_address(&self, _node_addr: &NodeAddr) -> Result<()> {
        Ok(())
    }

    fn subscribe(&self, _network_id: [u8; 32]) -> Option<BoxedStream<Result<DiscoveryEvent>>> {
        Some(Box::pin(self.rx.clone().into_stream()))
    }
}
