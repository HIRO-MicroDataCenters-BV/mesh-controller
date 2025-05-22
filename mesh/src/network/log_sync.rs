use std::collections::HashMap;

use async_trait::async_trait;
use p2panda_core::PublicKey;
use p2panda_net::TopicId;
use p2panda_sync::{TopicQuery, log_sync::TopicLogMap};
use serde::{Deserialize, Serialize};

pub type Logs<T> = HashMap<PublicKey, Vec<T>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct LogHeightTopic(String, [u8; 32]);

impl LogHeightTopic {
    pub fn new(name: &str) -> Self {
        Self(name.to_owned(), [0; 32])
    }
}

impl TopicQuery for LogHeightTopic {}

impl TopicId for LogHeightTopic {
    fn id(&self) -> [u8; 32] {
        self.1
    }
}
#[derive(Clone, Debug)]
pub struct LogHeightTopicMap<T>(HashMap<T, Logs<u64>>);

impl<T> LogHeightTopicMap<T>
where
    T: TopicQuery,
{
    pub fn new() -> Self {
        LogHeightTopicMap(HashMap::new())
    }

    pub fn insert(&mut self, topic_query: &T, logs: Logs<u64>) -> Option<Logs<u64>> {
        self.0.insert(topic_query.clone(), logs)
    }
}

#[async_trait]
impl<T> TopicLogMap<T, u64> for LogHeightTopicMap<T>
where
    T: TopicQuery,
{
    async fn get(&self, topic_query: &T) -> Option<Logs<u64>> {
        self.0.get(topic_query).cloned()
    }
}