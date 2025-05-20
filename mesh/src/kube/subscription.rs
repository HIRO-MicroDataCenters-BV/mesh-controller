use super::dynamic_object_ext::DynamicObjectExt;
use super::types::NamespacedName;
use super::{
    cache::{ResourceEntry, UID},
    types::CacheProtocol,
};
use crate::JoinErrToStr;
use crate::kube::cache::KindResources;
use anyhow::Result;
use anyhow::bail;
use anyhow::{Context, anyhow};
use dashmap::DashMap;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::future::MapErr;
use futures::future::Shared;
use kube::{Client, api::GroupVersionKind};
use kube::{
    api::{Api, DynamicObject, ResourceExt},
    runtime::watcher::{self, Event},
};
use std::time::Duration;
use std::{collections::BTreeMap, sync::Arc};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, info, trace};

const STREAM_RECONNECT_DELAY_MS: u64 = 1000;

pub struct Subscription {
    inner: Arc<SubscriptionInner>,
}

impl Subscription {
    pub fn new(
        subscriber_id: u32,
        client: Client,
        gvk: GroupVersionKind,
        tx: loole::Sender<CacheProtocol>,
        resources: Arc<KindResources>,
        cancelation: CancellationToken,
    ) -> Subscription {
        Subscription {
            inner: Arc::new(SubscriptionInner {
                subscriber_id,
                client,
                gvk,
                tx,
                resources,
                cancelation,
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

    pub fn stop(self) {
        self.inner.cancelation.cancel();
    }
}
pub struct SubscriptionInner {
    #[allow(dead_code)]
    subscriber_id: u32,
    client: Client,
    gvk: GroupVersionKind,
    tx: loole::Sender<CacheProtocol>,
    resources: Arc<KindResources>,
    cancelation: CancellationToken,
}

impl SubscriptionInner {
    async fn run_inner(&self) -> Result<()> {
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, &self.gvk).await?;
        let api = Api::<DynamicObject>::all_with(self.client.clone(), &ar);
        let wc = watcher::Config::default();
        let event_stream = watcher::watcher(api, wc);

        let mut events = event_stream.boxed();
        loop {
            tokio::select! {
                _ = self.cancelation.cancelled() => break,
                event = events.next() => {
                    trace!("event received {:?}", event);
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
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>,
        tx: &loole::Sender<CacheProtocol>,
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
        obj: DynamicObject,
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>,
        tx: &loole::Sender<CacheProtocol>,
        must_distribute: bool,
    ) -> Result<()> {
        let ns_name = obj.get_namespaced_name();
        let uid = obj
            .uid()
            .ok_or(anyhow!("UID is not set for dynamic object"))?;
        let version = obj
            .resource_version()
            .ok_or(anyhow!("resourceVersion is not set for dynamic object"))?
            .parse::<u64>()
            .context("unparsable resourceVersion in dynamic object")?;
        let resource = Arc::new(obj.clone());

        let resource_entries = resources.entry(ns_name).or_default();

        let updated = if let Some(mut existing) = resource_entries.get_mut(&uid) {
            if version > existing.value().version {
                *existing.value_mut() = ResourceEntry {
                    version,
                    resource,
                    tombstone: false,
                };
                true
            } else {
                false
            }
        } else {
            let entry = ResourceEntry {
                version,
                resource: resource.clone(),
                tombstone: false,
            };
            resource_entries.insert(uid, entry);
            true
        };

        if updated && must_distribute {
            tx.send(CacheProtocol::Update(obj))
                .unwrap_or_else(|e| error!("Failed to send update event: {}", e));
        }
        Ok(())
    }

    fn delete_resource(
        obj: DynamicObject,
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>,
        tx: &loole::Sender<CacheProtocol>,
    ) -> Result<()> {
        let ns_name = obj.get_namespaced_name();
        let uid = obj
            .uid()
            .ok_or(anyhow!("UID is not set for dynamic object"))?;
        let version = obj
            .resource_version()
            .ok_or(anyhow!("resourceVersion is not set for dynamic object"))?
            .parse::<u64>()
            .context("unparsable resourceVersion in dynamic object")?;
        let resource = Arc::new(obj.clone());

        let resource_entries = resources.entry(ns_name).or_default();

        let mut entry = resource_entries
            .entry(uid)
            .or_insert_with(|| ResourceEntry {
                version,
                resource: resource.clone(),
                tombstone: true,
            });
        if version > entry.value().version {
            *entry.value_mut() = ResourceEntry {
                version,
                resource,
                tombstone: true,
            };
            tx.send(CacheProtocol::Delete(obj))
                .unwrap_or_else(|e| error!("Failed to send delete event: {}", e));
        }
        Ok(())
    }

    fn send_snapshot(
        resources: &DashMap<NamespacedName, DashMap<UID, ResourceEntry>>,
        tx: &loole::Sender<CacheProtocol>,
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
                maybe_object.map(|object| (entry.key().to_owned(), object))
            })
            .collect::<BTreeMap<NamespacedName, Arc<DynamicObject>>>();

        tx.send(CacheProtocol::Snapshot { snapshot })
            .unwrap_or_else(|e| error!("Failed to send snapshot event: {}", e));

        Ok(())
    }
}
