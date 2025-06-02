use super::pool::{ObjectEntry, UID};
use super::dynamic_object_ext::DynamicObjectExt;
use super::event::KubeEvent;
use super::types::NamespacedName;
use crate::JoinErrToStr;
use crate::client::kube_client::KubeClient;
use crate::kube::pool::NamedObjects;
use anyhow::Result;
use anyhow::bail;
use anyhow::{Context, anyhow};
use dashmap::DashMap;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::future::MapErr;
use futures::future::Shared;
use kube::api::GroupVersionKind;
use kube::{
    api::{DynamicObject, ResourceExt},
    runtime::watcher::Event,
};
use std::time::Duration;
use std::{collections::BTreeMap, sync::Arc};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, info};

const STREAM_RECONNECT_DELAY_MS: u64 = 1000;

pub struct Subscription {
    inner: Arc<SubscriptionInner>,
}

impl Subscription {
    pub fn new(
        client: KubeClient,
        gvk: GroupVersionKind,
        tx: loole::Sender<KubeEvent>,
        resources: Arc<NamedObjects>,
        cancelation: CancellationToken,
        namespace: Option<String>,
    ) -> Subscription {
        Subscription {
            inner: Arc::new(SubscriptionInner {
                client,
                gvk,
                tx,
                resources,
                cancelation,
                namespace,
            }),
        }
    }
    pub fn run(&self) -> Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>> {
        let inner = self.inner.clone();

        let handle = tokio::spawn(async move {
            while !inner.cancelation.is_cancelled() {
                if let Err(error) = inner.run_inner().await {
                    tokio::time::sleep(Duration::from_millis(STREAM_RECONNECT_DELAY_MS)).await;
                    error!("Retrying subscription: {error}");
                } else {
                    info!("Stream stopped.");
                    break;
                }
            }
        });

        AbortOnDropHandle::new(handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared()
    }
}
pub struct SubscriptionInner {
    client: KubeClient,
    gvk: GroupVersionKind,
    namespace: Option<String>,
    tx: loole::Sender<KubeEvent>,
    resources: Arc<NamedObjects>,
    cancelation: CancellationToken,
}

impl SubscriptionInner {
    async fn run_inner(&self) -> Result<()> {
        let event_stream = self
            .client
            .event_stream_for(&self.gvk, &self.namespace)
            .await?;
        let mut events = event_stream.boxed();
        loop {
            tokio::select! {
                _ = self.cancelation.cancelled() => break,
                event = events.next() => {
                    match event {
                        Some(Ok(event)) => {
                            SubscriptionInner::handle_event(event, &self.resources, &self.tx)
                                .context("Subscription::handle_event failure")?;
                        },
                        Some(Err(e)) => bail!("Error in event stream {}", e),
                        None => break,
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_event(
        event: Event<DynamicObject>,
        resources: &DashMap<NamespacedName, DashMap<UID, ObjectEntry>>,
        tx: &loole::Sender<KubeEvent>,
    ) -> Result<()> {
        match event {
            Event::Init => Ok(()),
            Event::InitApply(obj) => SubscriptionInner::apply_resource(obj, resources, tx, false),
            Event::InitDone => SubscriptionInner::send_snapshot(resources, tx),
            Event::Apply(obj) => SubscriptionInner::apply_resource(obj, resources, tx, true),
            Event::Delete(obj) => SubscriptionInner::delete_resource(obj, resources, tx),
        }
    }

    fn apply_resource(
        object: DynamicObject,
        resources: &DashMap<NamespacedName, DashMap<UID, ObjectEntry>>,
        tx: &loole::Sender<KubeEvent>,
        must_distribute: bool,
    ) -> Result<()> {
        let ns_name = object.get_namespaced_name();
        let uid = object
            .uid()
            .ok_or(anyhow!("UID is not set for dynamic object"))?;
        let version = object
            .resource_version()
            .ok_or(anyhow!("resourceVersion is not set for dynamic object"))?
            .parse::<u64>()
            .context("unparsable resourceVersion in dynamic object")?;
        let resource = Arc::new(object.clone());
        let resource_entries = resources.entry(ns_name).or_default();

        let updated = if let Some(mut existing) = resource_entries.get_mut(&uid) {
            if version > existing.value().version {
                *existing.value_mut() = ObjectEntry {
                    version,
                    resource,
                    tombstone: false,
                };
                true
            } else {
                false
            }
        } else {
            let entry = ObjectEntry {
                version,
                resource: resource.clone(),
                tombstone: false,
            };
            resource_entries.insert(uid, entry);
            true
        };

        if updated && must_distribute {
            tx.send(KubeEvent::Update { version, object })
                .unwrap_or_else(|e| error!("Failed to send update event: {}", e));
        }
        Ok(())
    }

    fn delete_resource(
        object: DynamicObject,
        resources: &DashMap<NamespacedName, DashMap<UID, ObjectEntry>>,
        tx: &loole::Sender<KubeEvent>,
    ) -> Result<()> {
        let ns_name = object.get_namespaced_name();
        let uid = object
            .uid()
            .ok_or(anyhow!("UID is not set for dynamic object"))?;
        let version = object
            .resource_version()
            .ok_or(anyhow!("resourceVersion is not set for dynamic object"))?
            .parse::<u64>()
            .context("unparsable resourceVersion in dynamic object")?;
        let resource = Arc::new(object.clone());

        let resource_entries = resources.entry(ns_name).or_default();

        let mut entry = resource_entries.entry(uid).or_insert_with(|| ObjectEntry {
            version,
            resource: resource.clone(),
            tombstone: true,
        });
        if version > entry.value().version {
            *entry.value_mut() = ObjectEntry {
                version,
                resource,
                tombstone: true,
            };
            tx.send(KubeEvent::Delete { version, object })
                .unwrap_or_else(|e| error!("Failed to send delete event: {}", e));
        }
        Ok(())
    }

    fn send_snapshot(
        resources: &DashMap<NamespacedName, DashMap<UID, ObjectEntry>>,
        tx: &loole::Sender<KubeEvent>,
    ) -> Result<()> {
        let snapshot = resources
            .iter()
            .flat_map(|entry| {
                let maybe_object = entry
                    .value()
                    .iter()
                    .max_by_key(|e| e.value().version)
                    .and_then(|e| {
                        if e.tombstone {
                            None
                        } else {
                            Some(e.resource.to_owned())
                        }
                    });
                maybe_object.map(|object| (entry.key().to_owned(), object.as_ref().to_owned()))
            })
            .collect::<BTreeMap<NamespacedName, DynamicObject>>();
        let version = resources
            .iter()
            .flat_map(|entry| entry.value().iter().map(|e| e.value().version).max())
            .max()
            .unwrap_or(0);
        tx.send(KubeEvent::Snapshot { version, snapshot })
            .unwrap_or_else(|e| error!("Failed to send snapshot event: {}", e));

        Ok(())
    }
}
