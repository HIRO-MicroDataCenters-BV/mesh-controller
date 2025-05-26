use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashSet;
use p2panda_core::{Hash, PublicKey};
use p2panda_net::TopicId;
use p2panda_sync::{TopicQuery, log_sync::TopicLogMap};
use serde::{Deserialize, Serialize};

use super::kube_api::{Logs, MeshLogId};

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

impl TopicQuery for MeshTopic {}

impl TopicId for MeshTopic {
    fn id(&self) -> [u8; 32] {
        self.id
    }
}

#[derive(Debug, Clone)]
pub struct MeshTopicLogMap {
    inner: Arc<MeshTopicLogMapInner>
}

impl MeshTopicLogMap {
    pub fn new(owner: PublicKey) -> MeshTopicLogMap {
        MeshTopicLogMap { inner: Arc::new(MeshTopicLogMapInner {
            owner,
            peers: DashSet::new(),
        }) }
    }

    pub fn add_peer(&self, peer: PublicKey) {
        self.inner.peers.insert(peer);
    }
}

#[derive(Debug)]
struct MeshTopicLogMapInner {
    owner: PublicKey,
    peers: DashSet<PublicKey>,
}

#[async_trait]
impl TopicLogMap<MeshTopic, MeshLogId> for MeshTopicLogMap {
    async fn get(&self, _topic_query: &MeshTopic) -> Option<Logs<MeshLogId>> {
        let mut logs = Logs::new();
        self.inner.peers.iter().for_each(|peer| {
            logs.insert(peer.to_owned(), vec![MeshLogId()]);
        });
        logs.insert(self.inner.owner.to_owned(), vec![MeshLogId()]);
        Some(logs)
    }
}
