use crate::JoinErrToStr;

use super::{
    subscription::Subscription,
    types::{CacheError, CacheProtocol, NamespacedName},
};
use dashmap::DashMap;
use futures::StreamExt;
use kube::{
    api::{Api, DynamicObject, GroupVersionKind, PostParams, ResourceExt},
    runtime::watcher::{self, Event},
};

use anyhow::{anyhow, bail};
use futures::future::MapErr;
use futures::future::Shared;
use kube::Client;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, info};

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

    pub async fn get_direct(
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

    pub async fn update_direct(&self, resource: DynamicObject) -> anyhow::Result<()> {
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
    use anyhow::Result;
    use kube::api::{ApiResource, ObjectMeta};
    pub use serde::{Deserialize, Serialize};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_kube_cache() {
        setup_tracing(Some("=TRACE".to_string()));

        let service = FakeKubeApiService::new();

        let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication");
        let ar = ApiResource::from_gvk(&gvk);

        service.register(&ar);
        service.store(&gvk, anyapplication()).unwrap();

        let client = kube::Client::new(service, "default");

        // let client = custom_client().await.unwrap();

        // let client = kube::Client::try_default().await.unwrap();
        let cache = KubeCache::new(client);

        let subscriber = cache.subscribe(&gvk).await.unwrap();

        if let CacheProtocol::Snapshot { resources } = subscriber.recv().unwrap() {
            assert_eq!(resources.len(), 1);
            let (name, _) = resources.iter().next().unwrap();
            assert_eq!(name.name, "nginx-app");
            assert_eq!(name.namespace, "default");
        }
        // assert_eq!(item, CacheUpdateProtocol::Snapshot{ resources });

        assert!(!cache.unsubscribe(&gvk).await.is_err());

        cache.shutdown().unwrap();
    }

    fn anyapplication() -> DynamicObject {
        let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication");
        let ar = ApiResource::from_gvk(&gvk);

        let resource = AnyApplication {
            metadata: ObjectMeta {
                name: Some("test".into()),
                namespace: Some("namespace".into()),
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

    // #[tokio::test]
    // async fn test_kube_cache() {
    //     let service = FakeKubeApiService::new();
    //     let client = kube::Client::new(service, "default");
    //     let cache = KubeCache::new(client);
    //     let gvk = GroupVersionKind::gvk("", "", "");
    //     let subscriber = cache.subscribe(gvk).await.unwrap();

    //     cache.shutdown().unwrap();
    // }

    async fn custom_client() -> Result<Client> {
        use http::{Request, Response};
        use hyper::body::Incoming;
        use hyper_util::rt::TokioExecutor;
        use std::time::Duration;
        use tower::{BoxError, ServiceBuilder};
        // use tower_http::compression::DecompressionLayer;
        use bytes::Bytes;
        use tower_http::trace::TraceLayer;
        use tracing::{Span, *};

        use kube::{
            Api, Client, Config, ResourceExt,
            client::{Body, ConfigExt},
        };

        let config = Config::infer().await?;
        let https = config.rustls_https_connector()?;
        // let trace_layer = TraceLayer::new_for_http()
        //     .on_request(DefaultOnRequest::new().level(tracing::Level::INFO))
        //     .on_response(DefaultOnResponse::new().level(tracing::Level::INFO));

        let service = ServiceBuilder::new()
            .layer(config.base_uri_layer())
            .option_layer(config.auth_layer()?)
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(|request: &Request<Body>| {
                        tracing::debug_span!(
                            "HTTP",
                            http.method = %request.method(),
                            http.url = %request.uri(),
                            http.status_code = tracing::field::Empty,
                            otel.name = %format!("HTTP {}", request.method()),
                            otel.kind = "client",
                            otel.status_code = tracing::field::Empty,
                        )
                    })
                    .on_request(|request: &Request<Body>, _span: &Span| {
                        tracing::debug!(
                            "payload: {:?} headers: {:?}",
                            request.body(),
                            request.headers()
                        )
                    })
                    .on_response(
                        |response: &Response<Incoming>, latency: Duration, span: &Span| {
                            let status = response.status();
                            // let body = hyper::body::to_bytes(response.body()).await?;
                            span.record("http.status_code", status.as_u16());
                            if status.is_client_error() || status.is_server_error() {
                                span.record("otel.status_code", "ERROR");
                            }
                            // tracing::debug!("body {:?}", body);
                            tracing::debug!("finished in {}ms", latency.as_millis())
                        },
                    )
                    .on_body_chunk(|chunk: &Bytes, _latency: Duration, _span: &Span| {
                        debug!("response chunk: {:?}", chunk);
                    })
                    .on_eos(
                        |trailers: Option<&hyper::HeaderMap>,
                         _stream_duration: Duration,
                         _span: &Span| {
                            debug!("end of stream, trailers: {:?}", trailers);
                        },
                    ),
            )
            // .layer(ServiceBuilder::new().layer(trace_layer))
            // .layer(trace_layer)
            .map_err(BoxError::from)
            .service(
                hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(https),
            );

        let client = Client::new(service, config.default_namespace);
        Ok(client)
    }
}
