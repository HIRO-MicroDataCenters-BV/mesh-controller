use super::types::CacheError;
use crate::client::types::KubeClient;
use crate::kube::cache::watcher::Event;
use futures::Stream;
use kube::Resource;
use kube::api::{ApiResource, GroupVersionKind};
use kube::runtime::watcher;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

pub struct KubeCacheImpl<C>
where
    C: KubeClient,
{
    client: C,
}

impl<C> KubeCacheImpl<C>
where
    C: KubeClient,
{
    pub fn try_init(client: C) -> Result<Self, CacheError> {
        Ok(Self { client: client })
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
