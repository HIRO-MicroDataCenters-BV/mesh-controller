use std::collections::BTreeMap;

use anyhow::Result;
use kube::api::{DynamicObject, GroupVersionKind};

use crate::kube::{pool::Version, types::NamespacedName};

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
    },
    DoNothing,
    Conflict {
        msg: String,
    },
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
    DoNothing,
}

pub trait MergeStrategy: Send + Sync {
    fn local_update(
        &self,
        current: Option<DynamicObject>,
        incoming: DynamicObject,
        incoming_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult>;

    fn local_delete(
        &self,
        current: Option<DynamicObject>,
        incoming: DynamicObject,
        incoming_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult>;

    fn mesh_update(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult>;

    fn mesh_delete(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult>;

    fn is_owner_zone(&self, current: &DynamicObject, zone: &str) -> bool;
}
