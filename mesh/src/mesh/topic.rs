use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use p2panda_core::{Hash, PublicKey};
use p2panda_net::TopicId;
use p2panda_sync::{TopicQuery, log_sync::TopicLogMap};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, fmt::Debug};

use crate::merge::types::Membership;

pub type LogSeq = u64;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct MeshTopic {
    name: String,
    id: [u8; 32],
}

impl MeshTopic {
    pub fn new(name: &str) -> Self {
        let id = *Hash::new(name).as_bytes();
        Self {
            name: name.to_owned(),
            id,
        }
    }
}

impl Default for MeshTopic {
    fn default() -> Self {
        MeshTopic::new("mesh-network")
    }
}

impl TopicQuery for MeshTopic {}

impl TopicId for MeshTopic {
    fn id(&self) -> [u8; 32] {
        self.id
    }
}

#[derive(Debug, Clone)]
pub struct MeshTopicLogMap {
    inner: Arc<MeshTopicLogMapInner>,
}

impl MeshTopicLogMap {
    pub fn new(owner: PublicKey, log_id: MeshLogId) -> MeshTopicLogMap {
        MeshTopicLogMap {
            inner: Arc::new(MeshTopicLogMapInner {
                owner,
                log_id,
                peers: DashMap::new(),
                obsolete_logs: DashMap::new(),
            }),
        }
    }

    pub fn get_membership(&self) -> Membership {
        Membership::new()
    }

    pub fn add_peer(&self, peer: PublicKey) {
        self.inner.peers.entry(peer).or_default();
    }

    pub fn has_peer(&self, peer: &PublicKey) -> bool {
        self.inner.peers.contains_key(peer)
    }

    pub fn remove_peer(&self, peer: &PublicKey) {
        if peer == &self.inner.owner {
            // If the peer is the owner, log update is handled via restart of application.
            return;
        }
        if let Some((peer, Some(log_id))) = self.inner.peers.remove(peer) {
            self.inner
                .obsolete_logs
                .entry(peer)
                .or_default()
                .push(log_id);
        }
    }

    pub fn get_latest_log(&self, peer: &PublicKey) -> Option<MeshLogId> {
        if peer == &self.inner.owner {
            // If the peer is the owner, return the log_id of the owner.
            Some(self.inner.log_id.clone())
        } else {
            self.inner
                .peers
                .get(peer)
                .map(|e| e.value().to_owned())
                .unwrap_or(None)
        }
    }

    pub fn update_log(&self, peer: PublicKey, log_id: MeshLogId) {
        if peer == self.inner.owner {
            // If the peer is the owner, log update is handled via restart of application.
            return;
        }
        if let Some(Some(log_id)) = self.inner.peers.insert(peer, Some(log_id)) {
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
                    .as_ref()
                    .map(|log_id| (peer, log_id.to_owned()))
            })
            .collect()
    }
}

#[derive(Debug)]
struct MeshTopicLogMapInner {
    owner: PublicKey,
    log_id: MeshLogId,
    peers: DashMap<PublicKey, Option<MeshLogId>>,
    obsolete_logs: DashMap<PublicKey, Vec<MeshLogId>>,
}

#[async_trait]
impl TopicLogMap<MeshTopic, MeshLogId> for MeshTopicLogMap {
    async fn get(&self, _topic_query: &MeshTopic) -> Option<Logs<MeshLogId>> {
        let mut logs = Logs::new();
        self.inner.peers.iter().for_each(|peer| {
            let log_ids = peer
                .value()
                .as_ref()
                .map(|log_id| vec![log_id.clone()])
                .unwrap_or_default();
            logs.insert(peer.key().to_owned(), log_ids);
        });
        logs.insert(self.inner.owner.to_owned(), vec![self.inner.log_id.clone()]);
        Some(logs)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstanceId {
    pub zone: String,
    pub start_time: u64,
}

impl InstanceId {
    pub fn new(zone: String) -> InstanceId {
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("getting current time")
            .as_millis() as u64;
        InstanceId { zone, start_time }
    }
}

impl Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.zone, self.start_time)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MeshLogId(pub InstanceId);

impl Display for MeshLogId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogId({})", self.0)
    }
}

impl Default for MeshLogId {
    fn default() -> Self {
        MeshLogId(InstanceId {
            zone: "default".into(),
            start_time: 0,
        })
    }
}

pub type Logs<T> = HashMap<PublicKey, Vec<T>>;
