use std::sync::atomic::AtomicU64;

use crate::kube::{dynamic_object_ext::DynamicObjectExt, types::NamespacedName};
use anyhow::{Result, bail};
use dashmap::DashMap;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};

pub struct ResourceEntry {
    pub version: u64,
    pub resource: DynamicObject,
}

pub struct Storage {
    pub metadata: DashMap<GroupVersionKind, Vec<ApiResource>>,
    pub resources: DashMap<GroupVersionKind, DashMap<NamespacedName, ResourceEntry>>,
    resource_versions: AtomicU64,
    resource_uids: AtomicU64,
}

impl Storage {
    pub fn new() -> Storage {
        Storage {
            metadata: DashMap::new(),
            resources: DashMap::new(),
            resource_versions: AtomicU64::new(1),
            resource_uids: AtomicU64::new(1),
        }
    }

    pub fn register(&self, ar: &ApiResource) {
        let gvk = GroupVersionKind::gvk(&ar.group, &ar.version, &ar.kind);
        let mut entry = self.metadata.entry(gvk).or_insert_with(|| vec![]);
        entry.value_mut().push(ar.clone())
    }

    pub fn store(&self, mut resource: DynamicObject) -> Result<()> {
        let gvk = resource.get_gvk()?;
        if !self.metadata.contains_key(&gvk) {
            bail!("Resource {gvk:?} is not registred.");
        }
        let resources = self
            .resources
            .entry(gvk.to_owned())
            .or_insert_with(|| DashMap::new());
        let ns_name = resource.get_namespaced_name();

        let version = self
            .resource_versions
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        resource.metadata.resource_version = Some(version.to_string());
        if resource.metadata.uid.is_none() {
            let uid = self
                .resource_uids
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            resource.metadata.uid = Some(uid.to_string());
        }
        resources.insert(ns_name, ResourceEntry { version, resource });
        Ok(())
    }
}
