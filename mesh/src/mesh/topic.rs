use p2panda_core::{Hash, PublicKey};
use p2panda_net::TopicId;
use p2panda_sync::TopicQuery;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, fmt::Debug};

pub type LogSeq = u64;

#[derive(Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct MeshTopic {
    name: String,
    id: [u8; 32],
}

impl ::std::fmt::Display for MeshTopic {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "{}", ::hex::encode(&self.id))
    }
}

impl ::std::fmt::Debug for MeshTopic {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "{}({})", self.name, ::hex::encode(&self.id))
    }
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
        write!(f, "{}@{}", self.zone, self.start_time)
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
