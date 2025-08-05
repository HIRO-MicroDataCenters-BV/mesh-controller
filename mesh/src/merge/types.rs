use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use kube::api::{DynamicObject, GroupVersionKind};

use crate::{
    kube::{subscriptions::Version, types::NamespacedName},
    mesh::topic::InstanceId,
};

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
    Tombstone(Tombstone),
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
        membership: &Membership,
    ) -> Result<MergeResult>;

    fn mesh_delete(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_zone: &str,
        now_millis: u64,
    ) -> Result<MergeResult>;

    fn mesh_membership_change(
        &self,
        membership: &Membership,
        now_millis: u64,
    ) -> Result<Vec<MergeResult>>;

    fn tombstone(&self, current: VersionedObject, now_millis: u64) -> Result<Option<Tombstone>>;

    fn is_owner_zone(&self, current: &VersionedObject, zone: &str) -> bool;

    fn is_owner_zone_object(&self, current: &DynamicObject, zone: &str) -> bool;
}

#[derive(Debug, Clone)]
pub struct Membership {
    instances: HashMap<String, InstanceId>,
}

impl Membership {
    pub fn new() -> Membership {
        Membership {
            instances: HashMap::new(),
        }
    }
    pub fn get_instance(&self, zone: &str) -> Option<&InstanceId> {
        self.instances.get(zone)
    }
}
