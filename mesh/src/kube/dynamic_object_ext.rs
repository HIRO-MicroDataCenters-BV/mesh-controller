use anyhow::{Context, Result, anyhow};
use kube::ResourceExt;
use kube::api::{DynamicObject, GroupVersionKind};
use serde_json::{Value, json};

use super::pool::Version;
use super::types::NamespacedName;

const OWNER_VERSION: &str = "dcp.hiro.io/owner-version";
const OWNER_ZONE: &str = "dcp.hiro.io/owner-zone";

pub trait DynamicObjectExt {
    fn get_gvk(&self) -> Result<GroupVersionKind>;
    fn get_namespaced_name(&self) -> NamespacedName;
    fn get_first_item_or_fail(&self) -> Result<Option<DynamicObject>>;
    fn get_owner_version(&self) -> Result<Version>;
    fn set_owner_version(&mut self, version: Version);
    fn get_owner_zone(&self) -> Result<String>;
    fn set_owner_zone(&mut self, zone: String);
    fn normalize(&mut self, default_zone: &str);
    fn get_status(&self) -> Option<Value>;
}

impl DynamicObjectExt for DynamicObject {
    fn get_namespaced_name(&self) -> NamespacedName {
        let ns = self.namespace().unwrap_or("default".into());
        let name = self.name_any();
        NamespacedName::new(ns, name)
    }

    fn get_gvk(&self) -> Result<GroupVersionKind> {
        let types = self
            .types
            .as_ref()
            .ok_or_else(|| anyhow!("Missing TypeMeta in DynamicObject"))?;
        let api_version = &types.api_version;
        let kind = &types.kind;

        // Split apiVersion into group and version
        let (group, version) = if let Some((g, v)) = api_version.split_once('/') {
            (g.to_string(), v.to_string())
        } else {
            ("".to_string(), api_version.clone())
        };

        Ok(GroupVersionKind {
            group,
            version,
            kind: kind.clone(),
        })
    }

    fn get_first_item_or_fail(&self) -> Result<Option<DynamicObject>> {
        let gvk = self
            .get_gvk()
            .context("cannot get GroupVersionKind of result")?;
        if gvk.kind.ends_with("List") {
            let object = self
                .data
                .as_object()
                .ok_or(anyhow!("direct_get, 'items' object is expected"))?;
            let items = object
                .get("items")
                .and_then(|items| items.as_array())
                .ok_or(anyhow!("direct_get: items object is expected"))?;
            if items.is_empty() {
                return Ok(None);
            } else if items.len() > 1 {
                return Err(anyhow!("more than one item returned"));
            }

            let item = items
                .first()
                .cloned()
                .ok_or(anyhow!("single item is expected"))?;

            let object: DynamicObject = serde_json::from_str(&serde_json::to_string(&item)?)?;
            Ok(Some(object))
        } else {
            Ok(Some(self.clone()))
        }
    }

    fn get_owner_version(&self) -> Result<Version> {
        self.metadata
            .labels
            .as_ref()
            .ok_or(anyhow!("{} label not set", OWNER_VERSION))?
            .get(OWNER_VERSION)
            .map(|v| {
                v.parse::<Version>()
                    .map_err(|e| anyhow!("unable to parse version from label. {e}"))
            })
            .unwrap_or(Err(anyhow!("{} label not set", OWNER_VERSION)))
    }

    fn set_owner_version(&mut self, version: Version) {
        let labels = self.metadata.labels.get_or_insert_default();
        labels.insert(OWNER_VERSION.into(), version.to_string());
    }

    fn get_owner_zone(&self) -> Result<String> {
        self.metadata
            .labels
            .as_ref()
            .ok_or(anyhow!("{} label not set", OWNER_ZONE))?
            .get(OWNER_ZONE)
            .cloned()
            .ok_or(anyhow!("{} label not set", OWNER_ZONE))
    }

    fn set_owner_zone(&mut self, zone: String) {
        let labels = self.metadata.labels.get_or_insert_default();
        labels.insert(OWNER_ZONE.into(), zone.to_string());
    }

    fn normalize(&mut self, default_zone: &str) {
        self.metadata.managed_fields = None;
        self.metadata.uid = None;
        if self.get_owner_zone().is_err() {
            self.set_owner_zone(default_zone.into());
        }
    }

    fn get_status(&self) -> Option<Value> {
        self.data
            .get("status")
            .map(|status| json!({ "status": status }))
    }
}
