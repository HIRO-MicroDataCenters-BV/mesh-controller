use std::collections::BTreeMap;

use kube::api::DynamicObject;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Unknown error")]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct NamespacedName {
    pub namespace: String,
    pub name: String,
}

impl NamespacedName {
    pub fn new(namespace: String, name: String) -> Self {
        NamespacedName { namespace, name }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CacheUpdateProtocol {
    Update(DynamicObject),
    Delete(DynamicObject),
    Snapshot {
        resources: BTreeMap<NamespacedName, Arc<DynamicObject>>,
    },
}
