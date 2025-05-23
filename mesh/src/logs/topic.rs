use async_trait::async_trait;
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

#[derive(Debug)]
pub struct MeshTopicLogMap {
    owner: PublicKey,
}

impl MeshTopicLogMap {
    pub fn new(owner: PublicKey) -> MeshTopicLogMap {
        MeshTopicLogMap { owner }
    }
}

#[async_trait]
impl TopicLogMap<MeshTopic, MeshLogId> for MeshTopicLogMap {
    async fn get(&self, _topic_query: &MeshTopic) -> Option<Logs<MeshLogId>> {
        let mut logs = Logs::new();
        logs.insert(self.owner.to_owned(), vec![MeshLogId()]);
        Some(logs)
    }
}
