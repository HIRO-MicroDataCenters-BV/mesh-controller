use std::collections::BTreeMap;

use anyhow::Result;
use kube::api::{DynamicObject, GroupVersionKind};

use crate::kube::{subscriptions::Version, types::NamespacedName};

#[derive(Debug, Clone, PartialEq)]
pub enum MergeResult {
    Create { object: DynamicObject },
    Update { object: DynamicObject },
    Delete(Tombstone),
    Tombstone(Tombstone),
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
        tombstone: Tombstone,
    },
    Snapshot {
        snapshot: BTreeMap<NamespacedName, DynamicObject>,
    },
    Tombstone(Tombstone),
    Skip,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tombstone {
    pub gvk: GroupVersionKind,
    pub name: NamespacedName,
    pub owner_version: Version,
    pub owner_zone: String,
    pub resource_version: Version,
    pub deletion_timestamp: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum VersionedObject {
    NonExisting,
    Object(DynamicObject),
    Tombstone(Tombstone), // TODO add delete timestamp
}

impl From<DynamicObject> for VersionedObject {
    fn from(val: DynamicObject) -> Self {
        VersionedObject::Object(val)
    }
}

pub trait MergeStrategy: Send + Sync {
    fn kube_update(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_resource_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult>;

    fn kube_delete(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_resource_version: Version,
        incoming_zone: &str,
        now_millis: u64,
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
        now_millis: u64,
    ) -> Result<MergeResult>;

    fn is_owner_zone(&self, current: &VersionedObject, zone: &str) -> bool;

    fn is_owner_zone_object(&self, current: &DynamicObject, zone: &str) -> bool;
}
