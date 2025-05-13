use super::types::{CacheError, CacheUpdateProtocol, NamespacedName};
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
use std::{fmt::Debug, sync::Arc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};


type UID = String;
type Version = String;

pub struct ResourceEntry {
    version: Version,
    resource: Arc<DynamicObject>,
}

pub struct KubeCache {
    client: Client,
    token: CancellationToken,
    resources: Arc<DashMap<NamespacedName, DashMap<UID, ResourceEntry>>>,
}

impl KubeCache {
    pub fn new(client: Client) -> Result<Self, CacheError> {
        let token = CancellationToken::new();
        Ok(Self {
            client,
            token,
            resources: Arc::new(DashMap::new()),
        })
    }

    pub async fn get(&self, _gvk: &GroupVersionKind) -> Result<Option<DynamicObject>, CacheError> {
        unimplemented!()
    }

    pub async fn update(&self, resource: DynamicObject) -> Result<(), CacheError> {
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, &gvk).await?;
        let api = Api::<DynamicObject>::all_with(self.client.clone(), &ar);

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

    async fn handle_event_stream<S>(
        event_stream: S,
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>,
        cancelation: CancellationToken,
    ) -> Result<(), CacheError>
    where        
        S: Stream<Item = watcher::Result<Event<DynamicObject>>> + Send + 'static,
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
                            KubeCache::handle_event(event, &resources);
                        }
                        Some(Err(_e)) => {
                            // Handle error
                            return Err(CacheError::Unknown);
                        }
                        None => return Ok(()),
                    }
                }
            }
        }
    }

    fn handle_event(
        event: Event<DynamicObject>,
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>
    ) {
        match event {
            Event::Apply(obj) => KubeCache::apply_resource(obj, resources),
            Event::Delete(obj) => KubeCache::delete_resource(obj, resources),
            Event::InitApply(obj) => KubeCache::apply_resource(obj, resources),
            Event::Init | Event::InitDone => {}
        }
    }

    fn apply_resource(obj: DynamicObject, resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>) {
        let ns = obj.namespace().unwrap_or_default();
        let name = obj.name_any();
        let ns_name = NamespacedName::new(ns, name);
        let uid = obj.uid().unwrap();
        let version = obj.resource_version().unwrap_or_default();
        let resource = Arc::new(obj);

        let resource_entries =
            resources
                    .entry(ns_name)
                    .or_insert_with(|| DashMap::new());


        let mut entry =
            resource_entries
                .entry(uid)
                .or_insert_with(||ResourceEntry {
                    version: version.clone(),
                    resource: resource.clone(),
                });

        *entry.value_mut() = ResourceEntry {
            version,
            resource,
        };
        ()
    }

    fn delete_resource(obj: DynamicObject, resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>) {
        let ns = obj.namespace().unwrap_or_default();
        let name = obj.name_any();
        let ns_name = NamespacedName::new(ns, name);
        let uid = obj.uid().unwrap();
        let version = obj.resource_version().unwrap_or_default();
        let resource = Arc::new(obj);

        let resource_entries =
            resources
                    .entry(ns_name)
                    .or_insert_with(|| DashMap::new());


        let mut entry =
            resource_entries
                .entry(uid)
                .or_insert_with(||ResourceEntry {
                    version: version.clone(),
                    resource: resource.clone(),
                });

        *entry.value_mut() = ResourceEntry {
            version,
            resource,
        };
        ()
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
