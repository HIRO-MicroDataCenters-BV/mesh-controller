use crate::{JoinErrToStr, client::kube_client::KubeClient};

use super::{event::KubeEvent, subscription::Subscription};
use anyhow::Result;
use dashmap::DashMap;
use futures::future::MapErr;
use futures::future::Shared;
use kube::Error;
use kube::api::{DynamicObject, GroupVersionKind};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

pub type Version = u64;
pub type SubscriberId = u64;

#[derive(Debug, Clone)]
pub struct SubscriptionEntry {
    pub gvk: GroupVersionKind,
    pub pool_tx: loole::Sender<KubeEvent>,
    pub cancellation: CancellationToken,
    #[allow(dead_code)]
    handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

#[derive(Clone)]
pub struct Subscriptions {
    inner: Arc<RwLock<SubscriptionsInner>>,
    client: KubeClient,
    root: CancellationToken,
}

impl Subscriptions {
    pub fn new(client: KubeClient) -> Self {
        let root = CancellationToken::new();
        Self {
            inner: Arc::new(RwLock::new(SubscriptionsInner::new(
                client.clone(),
                root.clone(),
            ))),
            client,
            root,
        }
    }

    pub async fn subscribe(
        &self,
        gvk: &GroupVersionKind,
        namespace: &Option<String>,
    ) -> Result<(loole::Receiver<KubeEvent>, SubscriberId)> {
        self.inner.write().await.subscribe(gvk, namespace).await
    }

    pub async fn unsubscribe(&self, subscriber_id: &SubscriberId) -> anyhow::Result<()> {
        self.inner.write().await.unsubscribe(subscriber_id).await
    }

    pub fn shutdown(&self) -> Result<()> {
        self.root.cancel();
        Ok(())
    }

    pub fn client(&self) -> &KubeClient {
        &self.client
    }
}

pub struct SubscriptionsInner {
    subscriptions: DashMap<SubscriberId, SubscriptionEntry>,
    ids: SubscriberId,
    client: KubeClient,
    root: CancellationToken,
}

impl SubscriptionsInner {
    pub fn new(client: KubeClient, root: CancellationToken) -> Self {
        Self {
            subscriptions: DashMap::new(),
            ids: 0,
            root,
            client,
        }
    }

    pub async fn subscribe(
        &self,
        gvk: &GroupVersionKind,
        namespace: &Option<String>,
    ) -> Result<(loole::Receiver<KubeEvent>, SubscriberId)> {
        let (entry, subscriber_rx) = self.new_subscription(gvk, namespace);

        let id = self.ids + 1;
        self.subscriptions.insert(id, entry);

        Ok((subscriber_rx, id))
    }

    fn new_subscription(
        &self,
        gvk: &GroupVersionKind,
        namespace: &Option<String>,
    ) -> (SubscriptionEntry, loole::Receiver<KubeEvent>) {
        let (pool_tx, subscriber_rx) = loole::unbounded();

        let cancelation = self.root.child_token();
        let subscription = Subscription::new(
            self.client.to_owned(),
            gvk.to_owned(),
            pool_tx.to_owned(),
            cancelation.to_owned(),
            namespace.to_owned(),
        );
        let handle = subscription.run();
        let entry = SubscriptionEntry {
            gvk: gvk.to_owned(),
            pool_tx,
            cancellation: cancelation.to_owned(),
            handle,
        };
        (entry, subscriber_rx)
    }

    pub async fn unsubscribe(&self, subscriber_id: &SubscriberId) -> anyhow::Result<()> {
        if let Some((_, subscription_entry)) = self.subscriptions.remove(subscriber_id) {
            subscription_entry.cancellation.cancel();
            subscription_entry.pool_tx.close();
        }
        Ok(())
    }
}

pub fn object_not_found(results: &Result<DynamicObject, Error>) -> bool {
    match results {
        Err(Error::Api(response)) => response.reason == "NotFound",
        _ => false,
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::{kube::dynamic_object_ext::DynamicObjectExt, tracing::setup_tracing};
    use anyapplication::anyapplication::*;
    use anyhow::anyhow;
    use fake_kube_api::service::FakeKubeApiService;
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

        let cache = Subscriptions::new(KubeClient::build_fake(service));

        let (subscriber, id) = cache
            .subscribe(&gvk, &None)
            .await
            .expect("cache subscription");

        if let KubeEvent::Snapshot { snapshot, .. } =
            subscriber.recv().expect("receive snapshot event")
        {
            assert_eq!(snapshot.len(), 1);
            let (name, _) = snapshot.iter().next().unwrap();
            assert_eq!(name.name, "nginx-app");
            assert_eq!(name.namespace, "default");
        }

        subscriber.close();
        assert!(!cache.unsubscribe(&id).await.is_err());

        cache.shutdown().expect("cache shutdown");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_create() {
        setup_tracing(Some("=TRACE".to_string()));

        let service = FakeKubeApiService::new();

        let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication");
        let ar = ApiResource::from_gvk(&gvk);
        service.register(&ar);

        let client = KubeClient::build_fake(service);
        let cache = Subscriptions::new(client.clone());

        let resource = anyapplication();

        client
            .direct_patch_apply(resource.clone())
            .await
            .expect("resource is not updated");

        let created = client
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

        let client = KubeClient::build_fake(service);
        let cache = Subscriptions::new(client.clone());

        let mut to_update = client
            .direct_get(&gvk, &resource_name)
            .await
            .expect("cannot get resource")
            .ok_or(anyhow!("no result returned"))
            .unwrap();

        to_update.metadata.labels = Some(test_labels.clone());
        to_update.metadata.managed_fields = None;
        client
            .direct_patch_apply(to_update)
            .await
            .expect("resource is not updated");

        let updated = client
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

        let client = KubeClient::build_fake(service);
        let cache = Subscriptions::new(client.clone());

        let status = client
            .direct_delete(&gvk, &resource_name)
            .await
            .expect("resource is not deleted");
        info!("delete status {:?}", status);

        let deleted = client
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

        let client = KubeClient::build_fake(service);
        let cache = Subscriptions::new(client.clone());

        let (subscriber, id) = cache
            .subscribe(&gvk, &None)
            .await
            .expect("cache subscription");

        // Empty snapshot
        if let KubeEvent::Snapshot { snapshot, .. } =
            subscriber.recv().expect("receive snapshot event")
        {
            assert_eq!(snapshot.len(), 0);
        }

        // Create
        client
            .direct_patch_apply(resource)
            .await
            .expect("resource is not updated");
        assert!(matches!(
            subscriber.recv().expect("receive create event"),
            KubeEvent::Update { .. }
        ));

        // Update
        let mut to_update = client
            .direct_get(&gvk, &resource_name)
            .await
            .expect("cannot get resource")
            .ok_or(anyhow!("no result returned"))
            .unwrap();

        to_update.metadata.labels = Some(test_labels.clone());
        to_update.metadata.managed_fields = None;
        client
            .direct_patch_apply(to_update)
            .await
            .expect("resource is not updated");

        if let KubeEvent::Update { object, .. } = subscriber.recv().expect("receive update event") {
            assert_eq!(object.metadata.labels.unwrap(), test_labels);
        } else {
            panic!("invalid event received");
        }

        // Delete
        client
            .direct_delete(&gvk, &resource_name)
            .await
            .expect("resource is not deleted");
        assert!(matches!(
            subscriber.recv().unwrap(),
            KubeEvent::Delete { .. }
        ));
        subscriber.close();
        assert!(!cache.unsubscribe(&id).await.is_err());

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
