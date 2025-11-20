use std::pin::Pin;
use std::sync::Arc;
use std::{collections::BTreeMap, sync::atomic::AtomicU64};

use crate::dynamic_object_ext::{DynamicObjectExt, NamespacedName};
use anyhow::{Result, anyhow, bail};
use dashmap::DashMap;
use futures::StreamExt;
use futures::{Stream, future};
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, WatchEvent};
use kube::error::ErrorResponse;
use tokio::sync::RwLock;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio_stream::wrappers::BroadcastStream;
use tracing::error;

pub type ResourceVersion = u64;
pub type BoxedEventStream = Pin<Box<dyn Stream<Item = WatchEvent<DynamicObject>> + Send + 'static>>;

// Maximum number of events to keep in the changelog to prevent unbounded growth
const MAX_CHANGELOG_SIZE: usize = 10000;

pub struct ResourceEntry {
    pub version: ResourceVersion,
    pub resource: DynamicObject,
    pub tombstone: bool,
}

pub struct Storage {
    metadata: DashMap<GroupVersionKind, Vec<ApiResource>>,
    resources: DashMap<GroupVersionKind, Arc<DashMap<NamespacedName, ResourceEntry>>>,
    resource_versions: AtomicU64,
    resource_uids: AtomicU64,
    changelog: RwLock<BTreeMap<ResourceVersion, WatchEvent<DynamicObject>>>,
    event_tx: Sender<WatchEvent<DynamicObject>>,
    #[allow(dead_code)]
    event_rx: Receiver<WatchEvent<DynamicObject>>,
}

impl Storage {
    pub fn new() -> Storage {
        let (event_tx, event_rx) = tokio::sync::broadcast::channel(256);
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
        let resources = resources.value().clone();
        let ns_name = resource.get_namespaced_name();

        self.update_internal(resource, resources, &ns_name, |_, new| Ok(new))
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
        let api_resource = self.get_api_resource(group, version, kind_plural)?;
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
    }

    pub async fn watch_events(
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

        let events = BroadcastStream::new(self.event_tx.subscribe())
            .map(|result| match result {
                Ok(event) => event,
                Err(e) => watch_event_error(e),
            })
            .filter(move |e| future::ready(match_gvk(e, &gvk)));
        Ok(existing_events.chain(events).boxed())
    }

    pub async fn patch(
        &self,
        group: &str,
        version: &str,
        kind_plural: &str,
        ns_name: &NamespacedName,
        resource: DynamicObject,
    ) -> Result<DynamicObject> {
        let Some(api_resource) = self.get_api_resource(group, version, kind_plural) else {
            return Err(anyhow!("ApiResource is not registered"));
        };

        let gvk = GroupVersionKind::gvk(
            &api_resource.group,
            &api_resource.version,
            &api_resource.kind,
        );

        let Some(entry) = self.resources.get(&gvk) else {
            return Err(anyhow!(
                "Group, Vesion, Kind ({group},{version},{kind_plural}) is not registered"
            ));
        };

        let named_resources = entry.value().clone();
        drop(entry); // Drop the lock before calling update_internal

        self.update_internal(resource, named_resources, ns_name, |_, new| Ok(new))
            .await
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
        let Some(api_resource) = self.get_api_resource(group, version, kind_plural) else {
            return Err(anyhow!("ApiResource is not registered"));
        };

        let gvk = GroupVersionKind::gvk(
            &api_resource.group,
            &api_resource.version,
            &api_resource.kind,
        );

        let Some(entry) = self.resources.get(&gvk) else {
            return Err(anyhow!(
                "Group, Vesion, Kind ({group},{version},{kind_plural}) is not registered"
            ));
        };

        let named_resources = entry.value().clone();
        drop(entry); // Drop the lock before calling update_internal

        let update_fn =
            |current: Option<&DynamicObject>, new: DynamicObject| -> Result<DynamicObject> {
                match current {
                    Some(current) => {
                        let Some(status) = new.data.get(subresource) else {
                            return Err(anyhow!(
                                "No subresource {subresource} in source DynamicObject"
                            ));
                        };
                        let mut dst = current.clone();
                        let obj = dst.data.as_object_mut().unwrap();
                        obj.insert(subresource.to_string(), status.clone());
                        Ok(dst)
                    }
                    None => Err(anyhow!(
                        "patching subresource {subresource} of non existing object"
                    )),
                }
            };

        self.update_internal(object, named_resources, ns_name, update_fn)
            .await
    }

    pub async fn delete(
        &self,
        group: &str,
        version: &str,
        kind_plural: &str,
        ns_name: &NamespacedName,
    ) -> Result<DynamicObject> {
        let Some(api_resource) = self.get_api_resource(group, version, kind_plural) else {
            return Err(anyhow!("ApiResource is not registered"));
        };

        let gvk = GroupVersionKind::gvk(
            &api_resource.group,
            &api_resource.version,
            &api_resource.kind,
        );

        let Some(entry) = self.resources.get(&gvk) else {
            return Err(anyhow!(
                "Group, Vesion, Kind ({group},{version},{kind_plural}) is not registered"
            ));
        };

        let named_resources = entry.value().clone();
        drop(entry); // Drop the lock before calling delete_internal

        let deleted = self.delete_internal(named_resources, ns_name).await;
        deleted.ok_or(anyhow!("Object is not found"))
    }

    async fn update_internal<F>(
        &self,
        resource: DynamicObject,
        resources: Arc<DashMap<NamespacedName, ResourceEntry>>,
        ns_name: &NamespacedName,
        update_fn: F,
    ) -> Result<DynamicObject>
    where
        F: Fn(Option<&DynamicObject>, DynamicObject) -> Result<DynamicObject>,
    {
        let mut changelog = self.changelog.write().await;

        if let Some(mut ns_entry) = resources.get_mut(ns_name) {
            let entry = ns_entry.value_mut();
            if entry.tombstone {
                let mut updated = update_fn(None, resource)?;

                entry.tombstone = false;
                entry.version = self.update_version(&mut updated);
                entry.resource = updated.clone();

                let event = WatchEvent::Added(updated.clone());
                changelog.insert(entry.version, event.clone());
                Self::cleanup_changelog(&mut changelog);
                self.event_tx
                    .send(event)
                    .inspect_err(|e| error!("Failed to send event: {}", e))
                    .ok();
                Ok(updated)
            } else {
                let mut updated = update_fn(Some(&entry.resource), resource)?;

                entry.version = self.update_version(&mut updated);
                entry.resource = updated.clone();

                let event = WatchEvent::Modified(updated.clone());

                changelog.insert(entry.version, event.clone());
                Self::cleanup_changelog(&mut changelog);

                self.event_tx
                    .send(event)
                    .inspect_err(|e| error!("Failed to send event: {}", e))
                    .ok();
                Ok(updated)
            }
        } else {
            let mut updated = update_fn(None, resource)?;
            let new_version = self.update_version(&mut updated);
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
            Self::cleanup_changelog(&mut changelog);
            self.event_tx
                .send(event)
                .inspect_err(|e| error!("Failed to send event: {}", e))
                .ok();
            Ok(updated)
        }
    }

    async fn delete_internal(
        &self,
        resources: Arc<DashMap<NamespacedName, ResourceEntry>>,
        ns_name: &NamespacedName,
    ) -> Option<DynamicObject> {
        let mut changelog = self.changelog.write().await;

        let mut ns_entry = resources.get_mut(ns_name)?;

        let entry = ns_entry.value_mut();
        if entry.tombstone {
            None
        } else {
            let new_version = self.update_version(&mut entry.resource);
            entry.version = new_version;
            entry.tombstone = true;

            let event = WatchEvent::Deleted(entry.resource.clone());
            changelog.insert(new_version, event.clone());
            Self::cleanup_changelog(&mut changelog);
            self.event_tx
                .send(event)
                .inspect_err(|e| error!("Failed to send event: {}", e))
                .ok();
            Some(entry.resource.clone())
        }
    }

    fn update_version(&self, object: &mut DynamicObject) -> ResourceVersion {
        let new_version = self
            .resource_versions
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        object.metadata.resource_version = Some(new_version.to_string());
        if object.metadata.uid.is_none() {
            let uid = self
                .resource_uids
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            object.metadata.uid = Some(uid.to_string());
        }
        new_version
    }

    /// Cleanup old changelog entries if the size exceeds MAX_CHANGELOG_SIZE.
    /// Keeps the most recent entries.
    fn cleanup_changelog(changelog: &mut BTreeMap<ResourceVersion, WatchEvent<DynamicObject>>) {
        if changelog.len() > MAX_CHANGELOG_SIZE {
            // Calculate how many entries to remove
            let to_remove = changelog.len() - MAX_CHANGELOG_SIZE;
            // Get the keys of entries to remove (oldest ones)
            let keys_to_remove: Vec<ResourceVersion> = changelog
                .keys()
                .take(to_remove)
                .copied()
                .collect();
            // Remove the old entries
            for key in keys_to_remove {
                changelog.remove(&key);
            }
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
