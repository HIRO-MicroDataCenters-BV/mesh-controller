use anyhow::{Result, anyhow};
use kube::ResourceExt;
use kube::api::{DynamicObject, GroupVersionKind};
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NamespacedName {
    pub namespace: String,
    pub name: String,
}

impl NamespacedName {
    pub fn new(namespace: String, name: String) -> Self {
        NamespacedName { namespace, name }
    }
}

pub trait DynamicObjectExt {
    fn get_gvk(&self) -> Result<GroupVersionKind>;
    fn get_namespaced_name(&self) -> NamespacedName;
    fn generate_name(&mut self);
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

    fn generate_name(&mut self) {
        let token = self
            .metadata
            .generate_name
            .as_deref()
            .unwrap_or("generated");
        let rng = rand::rng();
        let random_token: String = rng
            .sample_iter(rand::distr::Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();
        let name = format!("{}-{}", token, random_token);
        self.metadata.name = Some(name);
    }
}
