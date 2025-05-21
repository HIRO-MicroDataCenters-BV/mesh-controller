use crate::JoinErrToStr;

use super::{
    subscription::Subscription,
    types::{CacheProtocol, NamespacedName},
};
use crate::kube::dynamic_object_ext::DynamicObjectExt;
use anyhow::Result;
use dashmap::DashMap;
use either::Either;
use futures::future::MapErr;
use futures::future::Shared;
use kube::Client;
use kube::Error;
use kube::api::{Api, DeleteParams, DynamicObject, GroupVersionKind, Patch, PatchParams};
use kube::client::Status;
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

// #[derive(Debug)]
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
    ) -> Result<Option<DynamicObject>> {
        let ns = &namspaced_name.namespace;
        let name = &namspaced_name.name;
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, gvk).await?;
        let api = Api::<DynamicObject>::namespaced_with(self.client.clone(), ns, &ar);
        let results = api.get(name).await;
        if object_not_found(&results) {
            Ok(None)
        } else {
            Ok(Some(results?))
        }
    }

    pub async fn direct_delete(
        &self,
        gvk: &GroupVersionKind,
        namspaced_name: &NamespacedName,
    ) -> Result<Either<DynamicObject, Status>> {
        let ns = &namspaced_name.namespace;
        let name = &namspaced_name.name;
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, gvk).await?;
        let api = Api::<DynamicObject>::namespaced_with(self.client.clone(), ns, &ar);
        let params = DeleteParams::default();
        let status = api.delete(name, &params).await?;
        Ok(status)
    }

    pub async fn direct_patch_apply(&self, resource: DynamicObject) -> Result<()> {
        let gvk = resource.get_gvk()?;
        let ns_name = resource.get_namespaced_name();

        let (ar, _) = kube::discovery::pinned_kind(&self.client, &gvk).await?;
        let api =
            Api::<DynamicObject>::namespaced_with(self.client.clone(), &ns_name.namespace, &ar);
        let patch_params = PatchParams::apply(&ns_name.name).force();

        api.patch(&ns_name.name, &patch_params, &Patch::Apply(resource))
            .await?;

        Ok(())
    }

    pub async fn subscribe(
        &self,
        gvk: &GroupVersionKind,
    ) -> Result<loole::Receiver<CacheProtocol>> {
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

    pub fn shutdown(&self) -> Result<()> {
        self.root.cancel();
        Ok(())
    }
}

fn object_not_found(results: &Result<DynamicObject, Error>) -> bool {
    match results {
        Err(Error::Api(response)) => response.reason == "NotFound",
        _ => false,
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
    use anyhow::anyhow;
    use kube::api::{ApiResource, ObjectMeta};
    pub use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;
    use tracing::info;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_receive_snapshot() {
        setup_tracing(Some("=TRACE".to_string()));

        let service = FakeKubeApiService::new();

        let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication");
        let ar = ApiResource::from_gvk(&gvk);

        service.register(&ar);
        service
            .store(anyapplication())
            .await
            .expect("saving kubernetes resource");

        let client = kube::Client::new(service, "default");
        let cache = KubeCache::new(client);

        let subscriber = cache.subscribe(&gvk).await.expect("cache subscription");

        if let CacheProtocol::Snapshot { snapshot } =
            subscriber.recv().expect("receive snapshot event")
        {
            assert_eq!(snapshot.len(), 1);
            let (name, _) = snapshot.iter().next().unwrap();
            assert_eq!(name.name, "nginx-app");
            assert_eq!(name.namespace, "default");
        }

        subscriber.close();
        assert!(!cache.unsubscribe(&gvk).await.is_err());

        cache.shutdown().expect("cache shutdown");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_create() {
        setup_tracing(Some("=TRACE".to_string()));

        let service = FakeKubeApiService::new();

        let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication");
        let ar = ApiResource::from_gvk(&gvk);
        service.register(&ar);

        let client = kube::Client::new(service, "default");
        let cache = KubeCache::new(client);

        let resource = anyapplication();

        cache
            .direct_patch_apply(resource.clone())
            .await
            .expect("resource is not updated");

        let created = cache
            .direct_get(&gvk, &resource.get_namespaced_name())
            .await
            .expect("cannot get resource");

        assert_eq!(created.map(|v| v.data), Some(resource.data));

        cache.shutdown().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_update() {
        setup_tracing(Some("=TRACE".to_string()));

        let service = FakeKubeApiService::new();
        let resource = anyapplication();
        let resource_name = resource.get_namespaced_name();
        let test_labels: BTreeMap<String, String> =
            BTreeMap::from([("test".into(), "test".into())]);

        let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication");
        let ar = ApiResource::from_gvk(&gvk);
        service.register(&ar);

        service
            .store(resource.clone())
            .await
            .expect("saving kubernetes resource");

        let client = kube::Client::new(service, "default");
        let cache = KubeCache::new(client);

        let mut to_update = cache
            .direct_get(&gvk, &resource_name)
            .await
            .expect("cannot get resource")
            .ok_or(anyhow!("no result returned"))
            .unwrap();

        to_update.metadata.labels = Some(test_labels.clone());
        to_update.metadata.managed_fields = None;
        cache
            .direct_patch_apply(to_update)
            .await
            .expect("resource is not updated");

        let updated = cache
            .direct_get(&gvk, &resource_name)
            .await
            .expect("cannot get resource")
            .ok_or(anyhow!("no result returned"))
            .unwrap();

        assert_eq!(updated.metadata.labels.unwrap(), test_labels);

        cache.shutdown().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_delete() {
        setup_tracing(Some("=TRACE".to_string()));

        let service = FakeKubeApiService::new();
        let resource = anyapplication();
        let resource_name = resource.get_namespaced_name();

        let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication");
        let ar = ApiResource::from_gvk(&gvk);
        service.register(&ar);

        service
            .store(resource.clone())
            .await
            .expect("saving kubernetes resource");

        let client = kube::Client::new(service, "default");
        let cache = KubeCache::new(client);

        let status = cache
            .direct_delete(&gvk, &resource_name)
            .await
            .expect("resource is not deleted");
        info!("delete status {:?}", status);

        let deleted = cache
            .direct_get(&gvk, &resource_name)
            .await
            .expect("cannot get resource");

        assert_eq!(deleted, None);

        cache.shutdown().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn test_lifecycle() {
        setup_tracing(Some("=TRACE".to_string()));

        let service = FakeKubeApiService::new();

        let test_labels: BTreeMap<String, String> =
            BTreeMap::from([("test".into(), "test".into())]);
        let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication");
        let ar = ApiResource::from_gvk(&gvk);
        let resource = anyapplication();
        let resource_name = resource.get_namespaced_name();

        service.register(&ar);

        let client = kube::Client::new(service, "default");
        let cache = KubeCache::new(client);

        let subscriber = cache.subscribe(&gvk).await.expect("cache subscription");

        // Empty snapshot
        if let CacheProtocol::Snapshot { snapshot } =
            subscriber.recv().expect("receive snapshot event")
        {
            assert_eq!(snapshot.len(), 0);
        }

        // Create
        cache
            .direct_patch_apply(resource)
            .await
            .expect("resource is not updated");
        assert!(matches!(
            subscriber.recv().expect("receive create event"),
            CacheProtocol::Update(_)
        ));

        // Update
        let mut to_update = cache
            .direct_get(&gvk, &resource_name)
            .await
            .expect("cannot get resource")
            .ok_or(anyhow!("no result returned"))
            .unwrap();

        to_update.metadata.labels = Some(test_labels.clone());
        to_update.metadata.managed_fields = None;
        cache
            .direct_patch_apply(to_update)
            .await
            .expect("resource is not updated");

        if let CacheProtocol::Update(updated) = subscriber.recv().expect("receive update event") {
            assert_eq!(updated.metadata.labels.unwrap(), test_labels);
        } else {
            panic!("invalid event received");
        }

        // Delete
        cache
            .direct_delete(&gvk, &resource_name)
            .await
            .expect("resource is not deleted");
        assert!(matches!(
            subscriber.recv().unwrap(),
            CacheProtocol::Delete(_)
        ));
        subscriber.close();
        assert!(!cache.unsubscribe(&gvk).await.is_err());

        cache.shutdown().expect("cache shutdown");
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
