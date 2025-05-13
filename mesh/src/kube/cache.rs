use super::types::{CacheError, CacheUpdateProtocol, NamespacedName};
use dashmap::DashMap;
use futures::StreamExt;
use kube::{
    api::{Api, DynamicObject, GroupVersionKind, PostParams, ResourceExt},
    runtime::watcher::{self, Event},
};

use kube::Client;

use anyhow::anyhow;
use tokio::sync::RwLock;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

type UID = String;
type Version = String;
type KindResources = DashMap<NamespacedName, DashMap<UID, ResourceEntry>>;

pub struct ResourceEntry {
    pub version: Version,
    pub resource: Arc<DynamicObject>,
}

pub struct SubscriptionEntry {
    pub pool_tx: loole::Sender<CacheUpdateProtocol>,
    pub subscriber_rx: loole::Receiver<CacheUpdateProtocol>,
    pub cancellation: CancellationToken,
}

pub struct KubeCache {
    resources: DashMap<GroupVersionKind, Arc<KindResources>>,
    subscriptions: DashMap<GroupVersionKind, SubscriptionEntry>,
    client: Client,
    root: CancellationToken,
    subscription_lock: RwLock<()>
}

impl KubeCache {
    pub fn new(client: Client) -> Self {
        Self {
            root: CancellationToken::new(),
            resources: DashMap::new(),
            subscriptions: DashMap::new(),
            subscription_lock: RwLock::new(()),
            client,
        }
    }

    pub async fn get(
        &self,
        gvk: &GroupVersionKind,
        namspaced_name: &NamespacedName,
    ) -> anyhow::Result<Option<DynamicObject>> {
        let ns = &namspaced_name.namespace;
        let name = &namspaced_name.name;
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, &gvk).await?;
        let api = Api::<DynamicObject>::namespaced_with(self.client.clone(), &ns, &ar);
        let result = api.get(name).await?;
        Ok(Some(result))
    }

    pub async fn update(&self, resource: DynamicObject) -> anyhow::Result<()> {
        let ns = resource.namespace().unwrap_or_default();
        let name = resource.name_any();
        let gvk = GroupVersionKind::gvk("", "", "");
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, &gvk).await?;
        let api = Api::<DynamicObject>::namespaced_with(self.client.clone(), &ns, &ar);
        let params = PostParams::default();

        api.replace(&name, &params, &resource).await?;

        Ok(())
    }

    pub async fn subscribe(
        &self,
        gvk: GroupVersionKind,
    ) -> anyhow::Result<loole::Receiver<CacheUpdateProtocol>> {

        let guard = self.subscription_lock.write().await;

        let subscription_entry = self.subscriptions.entry(gvk.clone()).or_insert_with(|| {
            let (pool_tx, subscriber_rx) = loole::unbounded();
            SubscriptionEntry {
                pool_tx,
                subscriber_rx,
                cancellation: self.root.child_token(),
            }
        });
        drop(guard);
        let subscription = subscription_entry.value();

        let resources = self
            .resources
            .entry(gvk.clone())
            .or_insert_with(|| Arc::new(DashMap::new()))
            .value()
            .clone();

        let token = subscription.cancellation.clone();
        let tx = subscription.pool_tx.clone();
        let rx = subscription.subscriber_rx.clone();
        let client = self.client.clone();

        drop(subscription_entry);

        tokio::spawn(async move {
            while !token.is_cancelled() {
                let stream_result =
                    KubeCache::run_subscription(&client, &gvk, &tx, &resources, token.clone())
                        .await;

                match stream_result {
                    Ok(_) => {
                        info!("Event stream ended successfully");
                        break;
                    }
                    Err(e) => {
                        error!("Error in event stream: {}, restarting", e);
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });
        Ok(rx)
    }

    pub fn unsubscribe(&self, gvk: GroupVersionKind) -> anyhow::Result<()> {
        Ok(())
    }

    async fn run_subscription(
        client: &Client,
        gvk: &GroupVersionKind,
        tx: &loole::Sender<CacheUpdateProtocol>,
        resources: &KindResources,
        cancelation: CancellationToken,
    ) -> anyhow::Result<()> {
        let (ar, _caps) = kube::discovery::pinned_kind(&client, &gvk).await?;
        let api = Api::<DynamicObject>::all_with(client.clone(), &ar);
        let wc = watcher::Config::default();
        let event_stream = watcher::watcher(api, wc);

        let mut events = event_stream.boxed();
        loop {
            tokio::select! {
                _ = cancelation.cancelled() => {
                    return Ok(());
                }
                event = events.next() => {
                    match event {
                        Some(Ok(event)) => {
                            KubeCache::handle_event(event, &resources, &tx);
                        }
                        Some(Err(_e)) => {
                            // Handle error
                            return Err(anyhow!("Error in event stream: {}", _e));
                        }
                        None => return Ok(()),
                    }
                }
            }
        }
    }

    fn handle_event(
        event: Event<DynamicObject>,
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>,
        tx: &loole::Sender<CacheUpdateProtocol>,
    ) {
        match event {
            Event::Apply(obj) => KubeCache::apply_resource(obj, resources),
            Event::Delete(obj) => KubeCache::delete_resource(obj, resources),
            Event::InitApply(obj) => KubeCache::apply_resource(obj, resources),
            Event::Init | Event::InitDone => {}
        }
    }

    fn apply_resource(
        obj: DynamicObject,
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>,
    ) {
        let ns = obj.namespace().unwrap_or_default();
        let name = obj.name_any();
        let ns_name = NamespacedName::new(ns, name);
        let uid = obj.uid().unwrap();
        let version = obj.resource_version().unwrap_or_default();
        let resource = Arc::new(obj);

        let resource_entries = resources.entry(ns_name).or_insert_with(|| DashMap::new());

        let mut entry = resource_entries
            .entry(uid)
            .or_insert_with(|| ResourceEntry {
                version: version.clone(),
                resource: resource.clone(),
            });

        *entry.value_mut() = ResourceEntry { version, resource };
    }

    fn delete_resource(
        obj: DynamicObject,
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>,
    ) {
        let ns = obj.namespace().unwrap_or_default();
        let name = obj.name_any();
        let ns_name = NamespacedName::new(ns, name);
        let uid = obj.uid().unwrap();
        let version = obj.resource_version().unwrap_or_default();
        let resource = Arc::new(obj);

        let resource_entries = resources.entry(ns_name).or_insert_with(|| DashMap::new());

        let mut entry = resource_entries
            .entry(uid)
            .or_insert_with(|| ResourceEntry {
                version: version.clone(),
                resource: resource.clone(),
            });

        *entry.value_mut() = ResourceEntry { version, resource };
        ()
    }

    pub fn shutdown(&self) -> Result<(), CacheError> {
        self.root.cancel();
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::{client::kube::FakeKubeApiService, kube::cache::KubeCache};

    #[tokio::test]
    async fn test_kube_cache() {
        let service = FakeKubeApiService::new();
        let client = kube::Client::new(service, "default");
        let cache = KubeCache::new(client);
        let gvk = GroupVersionKind::gvk("", "", "");
        let subscriber = cache.subscribe(gvk).await.unwrap();

        cache.shutdown().unwrap();
    }
}
