use std::collections::BTreeMap;

use anyhow::Result;
use kube::api::{DynamicObject, GroupVersionKind};
use meshkube::kube::{subscriptions::Version, types::NamespacedName};
use tracing::Span;

use crate::{mesh::event::MeshEvent, network::discovery::types::Membership};

#[derive(Debug, Clone, PartialEq)]
pub enum MergeResult {
    Create {
        object: DynamicObject,
    },
    Update {
        object: DynamicObject,
        event: Option<MeshEvent>,
    },
    Delete(Tombstone),
    Tombstone(Tombstone),
    Skip,
}

impl MergeResult {
    pub fn event_type(&self) -> &str {
        match self {
            MergeResult::Create { .. } => "create",
            MergeResult::Update { .. } => "update",
            MergeResult::Delete { .. } => "delete",
            MergeResult::Tombstone { .. } => "tombstone",
            MergeResult::Skip => "skip",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateResult {
    Create {
        version: Version,
        object: DynamicObject,
    },
    Update {
        version: Version,
        object: DynamicObject,
    },
    Delete {
        version: Version,
        object: DynamicObject,
        tombstone: Tombstone,
    },
    Snapshot {
        version: Version,
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
        span: &Span,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_resource_version: Version,
        node_zone: &str,
        now_millis: u64,
    ) -> Result<UpdateResult>;

    fn kube_delete(
        &self,
        span: &Span,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_resource_version: Version,
        node_zone: &str,
        now_millis: u64,
    ) -> Result<UpdateResult>;

    fn mesh_update(
        &self,
        span: &Span,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_zone: &str,
        current_zone: &str,
        membership: &Membership,
    ) -> Result<MergeResult>;

    fn mesh_delete(
        &self,
        span: &Span,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_zone: &str,
        now_millis: u64,
    ) -> Result<MergeResult>;

    fn mesh_membership_change(
        &self,
        span: &Span,
        current: VersionedObject,
        membership: &Membership,
        node_zone: &str,
    ) -> Result<Vec<MergeResult>>;

    fn tombstone(&self, current: VersionedObject, now_millis: u64) -> Result<Option<Tombstone>>;

    fn is_owner_zone(&self, current: &VersionedObject, zone: &str) -> bool;

    fn is_owner_zone_object(&self, current: &DynamicObject, zone: &str) -> bool;

    fn construct_remote_versions(
        &self,
        span: &Span,
        snapshot: &BTreeMap<NamespacedName, DynamicObject>,
        node_zone: &str,
    ) -> Result<BTreeMap<String, Version>>;
}
