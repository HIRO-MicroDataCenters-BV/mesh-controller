use dashmap::DashMap;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};

use crate::kube::types::NamespacedName;

pub struct ResourceEntry {
    pub version: u64,
    pub resource: DynamicObject,
}

pub struct Storage {
    pub metadata: DashMap<GroupVersionKind, Vec<ApiResource>>,
    pub resources: DashMap<GroupVersionKind, DashMap<NamespacedName, ResourceEntry>>,
}

impl Storage {
    pub fn new() -> Storage {
        Storage {
            metadata: DashMap::new(),
            resources: DashMap::new(),
        }
    }
}
