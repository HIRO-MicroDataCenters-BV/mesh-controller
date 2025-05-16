use anyhow::{Result, anyhow};
use kube::ResourceExt;
use kube::api::{DynamicObject, GroupVersionKind};

use super::types::NamespacedName;

pub trait DynamicObjectExt {
    fn get_gvk(&self) -> Result<GroupVersionKind>;
    fn get_namespaced_name(&self) -> NamespacedName;
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
}
