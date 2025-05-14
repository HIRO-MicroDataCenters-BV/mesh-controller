use super::types::{CacheError, CacheUpdateProtocol, NamespacedName};
use dashmap::DashMap;
use futures::StreamExt;
use kube::{
    api::{Api, DynamicObject, GroupVersionKind, PostParams, ResourceExt},
    runtime::watcher::{self, Event},
};

use kube::Client;

use anyhow::{anyhow, bail};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

type UID = String;
type Version = u64;
type KindResources = DashMap<NamespacedName, DashMap<UID, ResourceEntry>>;

#[derive(Debug, Clone, PartialEq)]
pub struct ResourceEntry {
    pub version: Version,
    pub resource: Arc<DynamicObject>,
    pub tombstone: bool
}

#[derive(Debug, Clone)]
pub struct SubscriptionEntry {
    pub pool_tx: loole::Sender<CacheUpdateProtocol>,
    pub subscriber_rx: loole::Receiver<CacheUpdateProtocol>,
    pub cancellation: CancellationToken,
    pub primary_subscriber: u32,
}

pub struct KubeCache {
    resources: DashMap<GroupVersionKind, Arc<KindResources>>,
    subscriptions: DashMap<GroupVersionKind, SubscriptionEntry>,
    client: Client,
    root: CancellationToken,
    subscriber_id: AtomicU32,
    subscription_lock: RwLock<()>,
}

impl KubeCache {
    pub fn new(client: Client) -> Self {
        Self {
            root: CancellationToken::new(),
            resources: DashMap::new(),
            subscriptions: DashMap::new(),
            subscription_lock: RwLock::new(()),
            subscriber_id: AtomicU32::new(1),
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
        gvk: &GroupVersionKind,
    ) -> anyhow::Result<loole::Receiver<CacheUpdateProtocol>> {
        let subscriber_id = self.subscriber_id.fetch_add(1, Ordering::AcqRel);
        let subscription_entry = self.subscriptions.entry(gvk.clone()).or_insert_with(|| {
            let (pool_tx, subscriber_rx) = loole::unbounded();
            SubscriptionEntry {
                pool_tx,
                subscriber_rx,
                cancellation: self.root.child_token(),
                primary_subscriber: subscriber_id,
            }
        });
        if subscriber_id != subscription_entry.primary_subscriber {
            Ok(subscription_entry.subscriber_rx.clone())
        } else {
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
            let gvk = gvk.clone();

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
    }

    pub fn unsubscribe(&self, gvk: &GroupVersionKind) -> anyhow::Result<()> {
        if let Some((_, subscription_entry)) = self.subscriptions.remove(gvk) {
            subscription_entry.cancellation.cancel();
            subscription_entry.pool_tx.close();
            self.resources.remove(gvk);
        }
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
                _ = cancelation.cancelled() => break,
                event = events.next() => {
                    match event {
                        Some(Ok(event)) => {
                            KubeCache::handle_event(event, &resources, &tx);
                        }
                        Some(Err(e)) => {
                            bail!("Error in event stream {}", e)
                        }
                        None => break,
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_event(
        event: Event<DynamicObject>,
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>,
        tx: &loole::Sender<CacheUpdateProtocol>,
    ) {
        match event {
            Event::Init => (),
            Event::InitApply(obj) => KubeCache::apply_resource(obj, resources, &tx, false),
            Event::InitDone => KubeCache::send_snapshot(resources, &tx),
            Event::Apply(obj) => KubeCache::apply_resource(obj, resources, &tx, true),
            Event::Delete(obj) => KubeCache::delete_resource(obj, resources, &tx),
        }
    }

    fn apply_resource(
        obj: DynamicObject,
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>,
        tx: &loole::Sender<CacheUpdateProtocol>,
        distribute: bool,
    ) {
        let ns = obj.namespace().unwrap_or_default();
        let name = obj.name_any();
        let ns_name = NamespacedName::new(ns, name);
        let uid = obj.uid().unwrap();
        let version = obj
            .resource_version()
            .map(|v| v.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let resource = Arc::new(obj.clone());

        let resource_entries = resources.entry(ns_name).or_insert_with(|| DashMap::new());

        let mut entry = resource_entries
            .entry(uid)
            .or_insert_with(|| ResourceEntry {
                version,
                resource: resource.clone(),
                tombstone: false
            });
        if version > entry.value().version {
            *entry.value_mut() = ResourceEntry { version, resource, tombstone: false };
            if distribute {
                let _ = tx.send(CacheUpdateProtocol::Update(obj));
            }
        }
    }

    fn delete_resource(
        obj: DynamicObject,
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>,
        tx: &loole::Sender<CacheUpdateProtocol>,
    ) {
        let ns = obj.namespace().unwrap_or_default();
        let name = obj.name_any();
        let ns_name = NamespacedName::new(ns, name);
        let uid = obj.uid().unwrap();
        let version = obj
            .resource_version()
            .map(|v| v.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let resource = Arc::new(obj.clone());

        let resource_entries = resources.entry(ns_name).or_insert_with(|| DashMap::new());

        let mut entry = resource_entries
            .entry(uid)
            .or_insert_with(|| ResourceEntry {
                version,
                resource: resource.clone(),
                tombstone: true
            });
        if version > entry.value().version {
            *entry.value_mut() = ResourceEntry { version, resource, tombstone: true };
            let _ = tx.send(CacheUpdateProtocol::Delete(obj));
        }
    }

    fn send_snapshot(
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>,
        tx: &loole::Sender<CacheUpdateProtocol>,
    ) {
        let snapshot = resources
            .iter()
            .flat_map(|entry| {
                let maybe_object = entry
                    .value()
                    .iter()
                    .max_by_key(|e| e.value().version)
                    .map(|e| {
                        if e.tombstone {
                            None
                        } else {
                            Some(e.resource.to_owned())
                        }
                    }).flatten();
                maybe_object
                    .map(|object|{
                        (entry.key().to_owned(), object)
                    })                
            })
            .collect::<BTreeMap<NamespacedName, Arc<DynamicObject>>>();
        let _ = tx.send(CacheUpdateProtocol::Snapshot {
            resources: snapshot,
        });
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_kube_cache() {
        //     let service = FakeKubeApiService::new();
        //     let client = kube::Client::new(service, "default");

        let client = kube::Client::try_default().await.unwrap();
        let cache = KubeCache::new(client);
        let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication");
        let subscriber = cache.subscribe(&gvk).await.unwrap();

        if let CacheUpdateProtocol::Snapshot { resources } = subscriber.recv().unwrap() {
            assert_eq!(resources.len(), 1);
            let (name, object) = resources.iter().next().unwrap();
            println!("{:?}", name);
            // assert_eq!(resources.g, 1);
        }
        // assert_eq!(item, CacheUpdateProtocol::Snapshot{ resources });

        assert!(!cache.unsubscribe(&gvk).is_err());

        cache.shutdown().unwrap();
    }

    // #[tokio::test]
    // async fn test_kube_cache() {
    //     let service = FakeKubeApiService::new();
    //     let client = kube::Client::new(service, "default");
    //     let cache = KubeCache::new(client);
    //     let gvk = GroupVersionKind::gvk("", "", "");
    //     let subscriber = cache.subscribe(gvk).await.unwrap();

    //     cache.shutdown().unwrap();
    // }
}
