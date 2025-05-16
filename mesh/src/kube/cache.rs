use crate::JoinErrToStr;

use super::{
    subscription::Subscription,
    types::{CacheError, CacheProtocol, NamespacedName},
};
use crate::kube::dynamic_object_ext::DynamicObjectExt;
use dashmap::DashMap;
use kube::api::{Api, DynamicObject, GroupVersionKind, PostParams};

use anyhow::Result;
use futures::future::MapErr;
use futures::future::Shared;
use kube::Client;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

pub type UID = String;
pub type Version = u64;
pub type KindResources = DashMap<NamespacedName, DashMap<UID, ResourceEntry>>;

#[derive(Debug, Clone, PartialEq)]
pub struct ResourceEntry {
    pub version: Version,
    pub resource: Arc<DynamicObject>,
    pub tombstone: bool,
}

#[derive(Debug, Clone)]
pub struct SubscriptionEntry {
    pub pool_tx: loole::Sender<CacheProtocol>,
    pub subscriber_rx: loole::Receiver<CacheProtocol>,
    pub cancellation: CancellationToken,
    pub primary_subscriber: u32,
    #[allow(dead_code)]
    handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

pub struct KubeCache {
    resources: DashMap<GroupVersionKind, Arc<KindResources>>,
    subscriptions: DashMap<GroupVersionKind, SubscriptionEntry>,
    client: Client,
    root: CancellationToken,
    subscriber_id: AtomicU32,
    mutable_lock: RwLock<()>,
}

impl KubeCache {
    pub fn new(client: Client) -> Self {
        Self {
            root: CancellationToken::new(),
            resources: DashMap::new(),
            subscriptions: DashMap::new(),
            mutable_lock: RwLock::new(()),
            subscriber_id: AtomicU32::new(1),
            client,
        }
    }

    pub async fn direct_get(
        &self,
        gvk: &GroupVersionKind,
        namspaced_name: &NamespacedName,
    ) -> Result<DynamicObject> {
        let ns = &namspaced_name.namespace;
        let name = &namspaced_name.name;
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, gvk).await?;
        let api = Api::<DynamicObject>::namespaced_with(self.client.clone(), ns, &ar);
        let result = api.get(name).await?;
        Ok(result)
    }

    pub async fn direct_update(&self, resource: DynamicObject) -> Result<()> {
        let gvk = resource.get_gvk()?;
        let ns_name = resource.get_namespaced_name();

        let (ar, _) = kube::discovery::pinned_kind(&self.client, &gvk).await?;
        let api =
            Api::<DynamicObject>::namespaced_with(self.client.clone(), &ns_name.namespace, &ar);

        let params = PostParams::default();
        api.replace(&ns_name.name, &params, &resource).await?;

        Ok(())
    }

    pub async fn subscribe(
        &self,
        gvk: &GroupVersionKind,
    ) -> anyhow::Result<loole::Receiver<CacheProtocol>> {
        let guard = self.mutable_lock.write().await;
        let subscriber_id = self.subscriber_id.fetch_add(1, Ordering::AcqRel);
        let subscription_entry = self.subscriptions.entry(gvk.clone()).or_insert_with(|| {
            let (pool_tx, subscriber_rx) = loole::unbounded();

            let resources = self
                .resources
                .entry(gvk.clone())
                .or_insert_with(|| Arc::new(DashMap::new()))
                .value()
                .clone();
            let cancelation = self.root.child_token();
            let subscription = Subscription::new(
                subscriber_id,
                self.client.to_owned(),
                gvk.to_owned(),
                pool_tx.to_owned(),
                resources,
                cancelation.to_owned(),
            );
            let handle = subscription.run();
            SubscriptionEntry {
                pool_tx,
                subscriber_rx,
                cancellation: cancelation.to_owned(),
                primary_subscriber: subscriber_id,
                handle,
            }
        });
        drop(guard);

        let subscriber_rx = subscription_entry.value().subscriber_rx.clone();
        Ok(subscriber_rx)
    }

    pub async fn unsubscribe(&self, gvk: &GroupVersionKind) -> anyhow::Result<()> {
        let _guard = self.mutable_lock.write().await;
        if let Some((_, subscription_entry)) = self.subscriptions.remove(gvk) {
            subscription_entry.cancellation.cancel();
            subscription_entry.pool_tx.close();
            self.resources.remove(gvk);
        }
        Ok(())
    }

    pub fn shutdown(&self) -> Result<(), CacheError> {
        self.root.cancel();
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::{
        client::service::FakeKubeApiService,
        kube::{
            anyapplication::{
                AnyApplication, AnyApplicationApplication, AnyApplicationApplicationHelm,
                AnyApplicationSpec, AnyApplicationStatus,
            },
            cache::KubeCache,
        },
        tracing::setup_tracing,
    };
    use kube::api::{ApiResource, ObjectMeta};
    pub use serde::{Deserialize, Serialize};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_snapshot() {
        setup_tracing(Some("=TRACE".to_string()));

        let service = FakeKubeApiService::new();

        let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication");
        let ar = ApiResource::from_gvk(&gvk);

        service.register(&ar);
        service.store(anyapplication()).unwrap();

        let client = kube::Client::new(service, "default");

        let cache = KubeCache::new(client);

        let subscriber = cache.subscribe(&gvk).await.unwrap();

        if let CacheProtocol::Snapshot { snapshot } = subscriber.recv().unwrap() {
            assert_eq!(snapshot.len(), 1);
            let (name, _) = snapshot.iter().next().unwrap();
            assert_eq!(name.name, "nginx-app");
            assert_eq!(name.namespace, "default");
        }

        assert!(!cache.unsubscribe(&gvk).await.is_err());

        cache.shutdown().unwrap();
    }

    fn anyapplication() -> DynamicObject {
        let resource = AnyApplication {
            metadata: ObjectMeta {
                name: Some("nginx-app".into()),
                namespace: Some("default".into()),
                ..Default::default()
            },
            spec: AnyApplicationSpec {
                application: AnyApplicationApplication {
                    helm: Some(AnyApplicationApplicationHelm {
                        chart: "chart".into(),
                        version: "1.0.0".into(),
                        namespace: "namespace".into(),
                        repository: "repo".into(),
                        values: None,
                    }),
                    resource_selector: None,
                },
                placement_strategy: None,
                recover_strategy: None,
                zones: 1,
            },
            status: Some(AnyApplicationStatus {
                conditions: None,
                owner: "owner".into(),
                placements: None,
                state: "New".into(),
            }),
        };
        let resource_str = serde_json::to_string(&resource).expect("Resource is not serializable");
        let object: DynamicObject = serde_json::from_str(&resource_str).unwrap();
        object
    }
}
