use std::collections::BTreeMap;

use anyhow::Result;
use kube::api::{DynamicObject, GroupVersionKind};

use crate::kube::{subscriptions::Version, types::NamespacedName};

#[derive(Debug, Clone, PartialEq)]
pub enum MergeResult {
    Create {
        object: DynamicObject,
    },
    Update {
        object: DynamicObject,
    },
    Delete {
        gvk: GroupVersionKind,
        name: NamespacedName,
        owner_version: Version,
        owner_zone: String,
        resource_version: Version,
    },
    Tombstone {
        name: NamespacedName,
        owner_version: Version,
        owner_zone: String,
    },
    Skip,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateResult {
    Create {
        object: DynamicObject,
    },
    Update {
        object: DynamicObject,
    },
    Delete {
        object: DynamicObject,
        // TODO should contain tombstone
        owner_version: Version,
        owner_zone: String,
    },
    Snapshot {
        snapshot: BTreeMap<NamespacedName, DynamicObject>,
    },
    Tombstone {
        name: NamespacedName,
        owner_version: Version,
        owner_zone: String,
    },
    Skip,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tombstone {
    gvk: GroupVersionKind,
    name: NamespacedName,
    owner_version: Version,
    owner_zone: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum VersionedObject {
    NonExisting,
    Object(DynamicObject),
    Tombstone(Version, String), // TODO add delete timestamp
}

impl From<DynamicObject> for VersionedObject {
    fn from(val: DynamicObject) -> Self {
        VersionedObject::Object(val)
    }
}

pub trait MergeStrategy: Send + Sync {
    fn local_update(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_resource_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult>;

    fn local_delete(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_resource_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult>;

    fn mesh_update(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_zone: &str,
        current_zone: &str,
    ) -> Result<MergeResult>;

    fn mesh_delete(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult>;

    fn is_owner_zone(&self, current: &VersionedObject, zone: &str) -> bool;

    fn is_owner_zone_object(&self, current: &DynamicObject, zone: &str) -> bool;
}
