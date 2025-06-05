use std::pin::Pin;
use std::{collections::BTreeMap, sync::atomic::AtomicU64};

use crate::dynamic_object_ext::{DynamicObjectExt, NamespacedName};
use anyhow::{Result, anyhow, bail};
use dashmap::DashMap;
use futures::Stream;
use futures::StreamExt;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, WatchEvent};
use kube::error::ErrorResponse;
use loole::Receiver;
use loole::Sender;
use tokio::sync::RwLock;
use tracing::error;

pub type ResourceVersion = u64;
pub type BoxedEventStream = Pin<Box<dyn Stream<Item = WatchEvent<DynamicObject>> + Send + 'static>>;

pub struct ResourceEntry {
    pub version: ResourceVersion,
    pub resource: DynamicObject,
    pub tombstone: bool,
}

pub struct Storage {
    metadata: DashMap<GroupVersionKind, Vec<ApiResource>>,
    resources: DashMap<GroupVersionKind, DashMap<NamespacedName, ResourceEntry>>,
    resource_versions: AtomicU64,
    resource_uids: AtomicU64,
    changelog: RwLock<BTreeMap<ResourceVersion, WatchEvent<DynamicObject>>>,
    event_tx: Sender<WatchEvent<DynamicObject>>,
    event_rx: Receiver<WatchEvent<DynamicObject>>,
}

impl Storage {
    pub fn new() -> Storage {
        let (event_tx, event_rx) = loole::unbounded();
        Storage {
            metadata: DashMap::new(),
            resources: DashMap::new(),
            resource_versions: AtomicU64::new(1),
            resource_uids: AtomicU64::new(1),
            changelog: RwLock::new(BTreeMap::new()),
            event_rx,
            event_tx,
        }
    }

    pub fn register(&self, ar: &ApiResource) {
        let gvk = GroupVersionKind::gvk(&ar.group, &ar.version, &ar.kind);
        let mut entry = self.metadata.entry(gvk.to_owned()).or_default();
        entry.value_mut().push(ar.clone());
        self.resources.entry(gvk.to_owned()).or_default();
    }

    pub async fn store(&self, resource: DynamicObject) -> Result<()> {
        let gvk = resource.get_gvk()?;
        if !self.metadata.contains_key(&gvk) {
            bail!("Resource {gvk:?} is not registred.");
        }
        let resources = self.resources.entry(gvk.to_owned()).or_default();

        let ns_name = resource.get_namespaced_name();

        self.update_internal(resource, resources.value(), &ns_name, |_, new| Ok(new))
            .await
            .map(|_| ())
    }

    pub fn get_api_resources(&self, group: &str, version: &str) -> Vec<ApiResource> {
        self.metadata
            .iter()
            .filter(|entry| entry.key().group == group && entry.key().version == version)
            .flat_map(|v| v.value().to_owned())
            .collect::<Vec<ApiResource>>()
    }

    pub fn get_api_resource(
        &self,
        group: &str,
        version: &str,
        kind_plural: &str,
    ) -> Option<ApiResource> {
        self.metadata
            .iter()
            .filter(|entry| entry.key().group == group && entry.key().version == version)
            .flat_map(|v| v.value().to_owned())
            .find(|ar| ar.plural == kind_plural)
    }

    pub fn find_objects(
        &self,
        group: &str,
        version: &str,
        kind_plural: &str,
    ) -> Option<(ApiResource, Vec<DynamicObject>)> {
        if let Some(api_resource) = self.get_api_resource(group, version, kind_plural) {
            let gvk = GroupVersionKind::gvk(
                &api_resource.group,
                &api_resource.version,
                &api_resource.kind,
            );

            let resources = self
                .resources
                .get(&gvk)
                .map(|v| {
                    v.value()
                        .iter()
                        .filter(|v| !v.tombstone)
                        .map(|v| v.value().resource.clone())
                        .collect()
                })
                .unwrap_or_default();
            Some((api_resource, resources))
        } else {
            None
        }
    }

    pub async fn subscribe(
        &self,
        group: &str,
        version: &str,
        kind_plural: &str,
        resource_version: u64,
    ) -> Result<BoxedEventStream> {
        let changelog = self.changelog.write().await;

        let gvk = self
            .get_api_resource(group, version, kind_plural)
            .map(|api_resource| {
                GroupVersionKind::gvk(
                    &api_resource.group,
                    &api_resource.version,
                    &api_resource.kind,
                )
            })
            .ok_or(anyhow!("Cannot find GroupVersionKind"))?;

        let items = changelog
            .range(resource_version..)
            .filter(|(_, e)| match_gvk(e, &gvk))
            .map(|(_, e)| e.to_owned())
            .collect::<Vec<WatchEvent<DynamicObject>>>();

        let existing_events = futures::stream::iter(items);

        let future_updates = futures::stream::unfold(
            (self.event_rx.clone(), false),
            move |(event_rx, is_terminated)| async move {
                if is_terminated {
                    None
                } else {
                    match event_rx.recv_async().await {
                        Ok(event) => Some((event, (event_rx, false))),
                        Err(e) => Some((watch_event_error(e), (event_rx, true))),
                    }
                }
            },
        );

        Ok(existing_events.chain(future_updates).boxed())
    }

    pub async fn create_or_update(
        &self,
        group: &str,
        version: &str,
        kind_plural: &str,
        ns_name: &NamespacedName,
        resource: DynamicObject,
    ) -> Result<DynamicObject> {
        if let Some(api_resource) = self.get_api_resource(group, version, kind_plural) {
            let gvk = GroupVersionKind::gvk(
                &api_resource.group,
                &api_resource.version,
                &api_resource.kind,
            );

            if let Some(entry) = self.resources.get_mut(&gvk) {
                let named_resources = entry.value();
                self.update_internal(resource, named_resources, ns_name, |_, new| Ok(new))
                    .await
            } else {
                Err(anyhow!(
                    "Group, Vesion, Kind ({group},{version},{kind_plural}) is not registered"
                ))
            }
        } else {
            Err(anyhow!("ApiResource is not registered"))
        }
    }

    pub async fn patch_subresource(
        &self,
        group: &str,
        version: &str,
        kind_plural: &str,
        subresource: &str,
        ns_name: &NamespacedName,
        object: DynamicObject,
    ) -> Result<DynamicObject> {
        if let Some(api_resource) = self.get_api_resource(group, version, kind_plural) {
            let gvk = GroupVersionKind::gvk(
                &api_resource.group,
                &api_resource.version,
                &api_resource.kind,
            );

            if let Some(entry) = self.resources.get_mut(&gvk) {
                let named_resources = entry.value();
                let update_fn = |current: Option<&DynamicObject>,
                                 new: DynamicObject|
                 -> Result<DynamicObject> {
                    match current {
                        Some(current) => {
                            if let Some(status) = new.data.get(subresource) {
                                let mut dst = current.clone();
                                let obj = dst.data.as_object_mut().unwrap();
                                obj.insert(subresource.to_string(), status.clone());
                                Ok(dst)
                            } else {
                                Err(anyhow!(
                                    "No subresource {subresource} in source DynamicObject"
                                ))
                            }
                        }
                        None => Err(anyhow!(
                            "patching subresource {subresource} of non existing object"
                        )),
                    }
                };
                self.update_internal(object, named_resources, ns_name, update_fn)
                    .await
            } else {
                Err(anyhow!(
                    "Group, Vesion, Kind ({group},{version},{kind_plural}) is not registered"
                ))
            }
        } else {
            Err(anyhow!("ApiResource is not registered"))
        }
    }

    pub async fn delete(
        &self,
        group: &str,
        version: &str,
        kind_plural: &str,
        ns_name: &NamespacedName,
    ) -> Result<DynamicObject> {
        if let Some(api_resource) = self.get_api_resource(group, version, kind_plural) {
            let gvk = GroupVersionKind::gvk(
                &api_resource.group,
                &api_resource.version,
                &api_resource.kind,
            );

            if let Some(entry) = self.resources.get_mut(&gvk) {
                let named_resources = entry.value();
                let deleted = self.delete_internal(named_resources, ns_name).await;
                deleted.ok_or(anyhow!("Object is not found"))
            } else {
                Err(anyhow!(
                    "Group, Vesion, Kind ({group},{version},{kind_plural}) is not registered"
                ))
            }
        } else {
            Err(anyhow!("ApiResource is not registered"))
        }
    }

    async fn update_internal<F>(
        &self,
        mut resource: DynamicObject,
        resources: &DashMap<NamespacedName, ResourceEntry>,
        ns_name: &NamespacedName,
        update_fn: F,
    ) -> Result<DynamicObject>
    where
        F: Fn(Option<&DynamicObject>, DynamicObject) -> Result<DynamicObject>,
    {
        let mut changelog = self.changelog.write().await;
        let new_version = self
            .resource_versions
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        resource.metadata.resource_version = Some(new_version.to_string());
        if resource.metadata.uid.is_none() {
            let uid = self
                .resource_uids
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            resource.metadata.uid = Some(uid.to_string());
        }
        if let Some(mut ns_entry) = resources.get_mut(ns_name) {
            let entry = ns_entry.value_mut();
            if entry.tombstone {
                let updated = update_fn(None, resource)?;
                entry.tombstone = false;
                entry.version = new_version;
                entry.resource = updated.clone();
                let event = WatchEvent::Added(updated.clone());
                changelog.insert(new_version, event.clone());
                self.event_tx
                    .send(event)
                    .unwrap_or_else(|e| error!("Failed to send event: {}", e));
                Ok(updated)
            } else {
                let updated = update_fn(Some(&entry.resource), resource)?;
                entry.version = new_version;
                entry.resource = updated.clone();
                let event = WatchEvent::Modified(updated.clone());
                changelog.insert(new_version, event.clone());
                self.event_tx
                    .send(event)
                    .unwrap_or_else(|e| error!("Failed to send event: {}", e));
                Ok(updated)
            }
        } else {
            let updated = update_fn(None, resource)?;
            resources.insert(
                ns_name.to_owned(),
                ResourceEntry {
                    version: new_version,
                    resource: updated.clone(),
                    tombstone: false,
                },
            );
            let event = WatchEvent::Added(updated.clone());
            changelog.insert(new_version, event.clone());
            self.event_tx
                .send(event)
                .unwrap_or_else(|e| error!("Failed to send event: {}", e));
            Ok(updated)
        }
    }

    async fn delete_internal(
        &self,
        resources: &DashMap<NamespacedName, ResourceEntry>,
        ns_name: &NamespacedName,
    ) -> Option<DynamicObject> {
        let mut changelog = self.changelog.write().await;

        if let Some(mut ns_entry) = resources.get_mut(ns_name) {
            let entry = ns_entry.value_mut();
            if entry.tombstone {
                None
            } else {
                let resource = &mut entry.resource;

                let new_version = self
                    .resource_versions
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                resource.metadata.resource_version = Some(new_version.to_string());
                if resource.metadata.uid.is_none() {
                    let uid = self
                        .resource_uids
                        .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    resource.metadata.uid = Some(uid.to_string());
                }

                entry.version = new_version;
                entry.tombstone = true;

                let event = WatchEvent::Modified(resource.clone());
                changelog.insert(new_version, event.clone());
                self.event_tx
                    .send(event)
                    .unwrap_or_else(|e| error!("Failed to send event: {}", e));
                Some(resource.clone())
            }
        } else {
            None
        }
    }
}

fn watch_event_error<K, E: std::error::Error>(error: E) -> WatchEvent<K> {
    WatchEvent::Error(ErrorResponse {
        status: "FAILED".into(),
        message: format!("receiver error {:?}", error),
        reason: "Error in stream".into(),
        code: 0,
    })
}

fn match_gvk(event: &WatchEvent<DynamicObject>, gvk: &GroupVersionKind) -> bool {
    fn match_object(obj: &DynamicObject, gvk: &GroupVersionKind) -> bool {
        match obj.get_gvk() {
            Ok(g) => &g == gvk,
            Err(_) => false,
        }
    }
    match event {
        WatchEvent::Added(obj) | WatchEvent::Modified(obj) | WatchEvent::Deleted(obj) => {
            match_object(obj, gvk)
        }
        WatchEvent::Bookmark(_) | WatchEvent::Error(_) => true,
    }
}
