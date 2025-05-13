use super::types::{CacheError, NamespacedName};
use dashmap::DashMap;
use futures::{Stream, StreamExt, TryStreamExt};
use kube::{
    api::{Api, ApiResource, DynamicObject, GroupVersionKind, Resource, ResourceExt},
    core::DynamicResourceScope,
    runtime::{
        WatchStreamExt,
        watcher::{self, Event},
    },
};

use kube::Client;

use serde::de::DeserializeOwned;
use std::fmt::Debug;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

type KubeResource = dyn Resource<DynamicType = ApiResource, Scope = DynamicResourceScope>;

pub struct ResourceEntry {
    uid: String,
}

pub struct KubeCache {
    client: Client,
    token: CancellationToken,
    resources: DashMap<NamespacedName, ()>,
}

impl KubeCache {
    pub fn try_init(client: Client) -> Result<Self, CacheError> {
        let token = CancellationToken::new();
        Ok(Self {
            client,
            token,
            resources: DashMap::new(),
        })
    }

    pub async fn get<R>(_gvk: &GroupVersionKind) -> Result<Option<R>, CacheError>
    where
        R: Resource<DynamicType = ApiResource> + Clone + Debug + Send + DeserializeOwned + 'static,
    {
        unimplemented!()
    }

    pub async fn update<R>(_resource: R) -> Result<(), CacheError>
    where
        R: Resource<DynamicType = ApiResource> + Clone + Debug + Send + DeserializeOwned + 'static,
    {
        unimplemented!()
    }

    pub async fn subscribe<R>(&self, gvk: &GroupVersionKind) -> anyhow::Result<()>
    where
        R: Resource<DynamicType = ApiResource> + Clone + Debug + Send + DeserializeOwned + 'static,
    {
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, &gvk).await?;
        let api = Api::<DynamicObject>::all_with(self.client.clone(), &ar);
        let wc = watcher::Config::default();
        tokio::spawn({
            let token = self.token.clone();
            let resources = self.resources.clone();
            async move {
                if let Err(e) = KubeCache::handle_event_stream(
                    watcher::watcher(api, wc),
                    &ar,
                    &resources,
                    token,
                )
                .await
                {
                    error!("Error in event stream: {}", e);
                }
            }
        });
        Ok(())
    }

    async fn handle_event_stream<R, S>(
        event_stream: S,
        ar: &ApiResource,
        resources: &DashMap<NamespacedName, ()>,
        cancelation: CancellationToken,
    ) -> Result<(), CacheError>
    where
        R: Resource<DynamicType = ApiResource> + Clone + Debug + Send + DeserializeOwned + 'static,
        S: Stream<Item = watcher::Result<Event<R>>> + Send + 'static,
    {
        let mut events = event_stream.boxed();
        loop {
            tokio::select! {
                _ = cancelation.cancelled() => {
                    return Ok(());
                }
                event = events.next() => {
                    match event {
                        Some(Ok(event)) => {
                            KubeCache::handle_event(event, ar, &resources);
                        }
                        Some(Err(e)) => {
                            // Handle error
                            return Err(CacheError::Unknown);
                        }
                        None => return Ok(()),
                    }
                }
            }
        }
    }

    fn handle_event<R>(event: Event<R>, ar: &ApiResource, resources: &DashMap<NamespacedName, ()>)
    where
        R: Resource<DynamicType = ApiResource> + Clone + Debug + Send + DeserializeOwned + 'static,
    {
        match event {
            Event::Apply(obj) => {
                let ns = obj.namespace().unwrap_or_default();
                let name = obj.name_any();
                info!("saw {} {} in {ns}", R::kind(ar), name);
                resources.insert(NamespacedName::new(ns, name), ());
            }
            Event::Delete(obj) => {
                let ns = obj.namespace().unwrap_or_default();
                let name = obj.name_any();
                info!("saw {} {} in {ns}", R::kind(ar), name);
                resources.insert(NamespacedName::new(ns, name), ());
            }
            Event::Init => {}
            Event::InitApply(K) => {}
            Event::InitDone => {}
        }
    }

    pub fn shutdown(&self) -> Result<(), CacheError> {
        self.token.cancel();
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    // use crate::client::types::KubeClientImpl;
    use crate::kube::cache::KubeCache;

    #[test]
    fn test_kube_cache() {
        // let client = KubeClientImpl::try_init().unwrap();
        // let kube_cache = KubeCacheImpl::try_init(client).unwrap();
        // assert!(kube_cache.shutdown().is_ok());
    }
}
