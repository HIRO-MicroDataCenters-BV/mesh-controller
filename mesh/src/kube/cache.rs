use super::types::CacheError;
use crate::client::types::KubeClient;
use crate::kube::cache::watcher::Event;
use futures::Stream;
use kube::Client;
use kube::Resource;
use kube::api::{ApiResource, GroupVersionKind};
use kube::runtime::watcher;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

pub struct KubeCacheImpl {
    client: Client,
}

impl KubeCacheImpl {
    pub fn try_init(client: Client) -> Result<Self, CacheError> {
        Ok(Self { client })
    }

    pub async fn get<R>(gvk: &GroupVersionKind) -> Result<Option<R>, CacheError>
    where
        R: Resource<DynamicType = ApiResource> + Clone + Debug + Send + DeserializeOwned + 'static,
    {
        unimplemented!()
    }

    pub async fn update<R>(resource: R) -> Result<(), CacheError>
    where
        R: Resource<DynamicType = ApiResource> + Clone + Debug + Send + DeserializeOwned + 'static,
    {
        unimplemented!()
    }

    pub fn handle_event<R, S>(event_stream: S) -> Result<(), CacheError>
    where
        R: Resource<DynamicType = ApiResource> + Clone + Debug + Send + DeserializeOwned + 'static,
        S: Stream<Item = watcher::Result<Event<R>>> + Send + 'static,
    {
        unimplemented!()
    }

    pub fn shutdown(&self) -> Result<(), CacheError> {
        unimplemented!()
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    // use crate::client::types::KubeClientImpl;
    use crate::kube::cache::KubeCacheImpl;

    #[test]
    fn test_kube_cache() {
        // let client = KubeClientImpl::try_init().unwrap();
        // let kube_cache = KubeCacheImpl::try_init(client).unwrap();
        // assert!(kube_cache.shutdown().is_ok());
    }
}
