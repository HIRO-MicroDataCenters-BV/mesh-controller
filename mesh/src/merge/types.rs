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
        version: Version,
        owner_zone: String,
    },
    Tombstone {
        name: NamespacedName,
        owner_version: Version,
        owner_zone: String,
    },
    DoNothing,
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
    },
    Snapshot {
        snapshot: BTreeMap<NamespacedName, DynamicObject>,
    },
    Tombstone {
        name: NamespacedName,
        owner_version: Version,
        owner_zone: String,
    },
    DoNothing,
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

impl Into<VersionedObject> for DynamicObject {
    fn into(self) -> VersionedObject {
        VersionedObject::Object(self)
    }
}

pub trait MergeStrategy: Send + Sync {
    fn local_update(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult>;

    fn local_delete(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_version: Version,
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
