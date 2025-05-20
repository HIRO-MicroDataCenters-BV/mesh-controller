use anyhow::{Context, Result, anyhow};
use kube::ResourceExt;
use kube::api::{DynamicObject, GroupVersionKind};

use super::types::NamespacedName;

pub trait DynamicObjectExt {
    fn get_gvk(&self) -> Result<GroupVersionKind>;
    fn get_namespaced_name(&self) -> NamespacedName;
    fn get_first_item_or_fail(&self) -> Result<Option<DynamicObject>>;
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
}
