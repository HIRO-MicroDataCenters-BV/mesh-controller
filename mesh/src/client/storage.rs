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
    metadata: DashMap<GroupVersionKind, Vec<ApiResource>>,
    resources: DashMap<GroupVersionKind, DashMap<NamespacedName, ResourceEntry>>,
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
                        .map(|v| v.value().resource.clone())
                        .collect()
                })
                .unwrap_or_default();
            Some((api_resource, resources))
        } else {
            None
        }
    }
}
