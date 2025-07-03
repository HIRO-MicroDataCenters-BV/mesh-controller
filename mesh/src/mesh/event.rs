use std::{collections::BTreeMap, fmt::Display};

use kube::api::DynamicObject;
use serde::{Deserialize, Serialize};

use crate::{
    kube::{dynamic_object_ext::DynamicObjectExt, types::NamespacedName},
    merge::types::UpdateResult,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MeshEvent {
    Update {
        object: DynamicObject,
    },
    Delete {
        object: DynamicObject,
    },
    Snapshot {
        snapshot: BTreeMap<NamespacedName, DynamicObject>,
    },
}

impl MeshEvent {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("encoding network message");
        bytes
    }
}

impl TryFrom<Vec<u8>> for MeshEvent {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        ciborium::from_reader(&bytes[..])
    }
}

impl Display for MeshEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MeshEvent::Update { object } => {
                write!(f, "Update({:?})", object.get_namespaced_name())
            }
            MeshEvent::Delete { object } => {
                write!(f, "Delete({:?})", object.get_namespaced_name())
            }
            MeshEvent::Snapshot { snapshot } => {
                write!(f, "Snapshot({} items)", snapshot.len())
            }
        }
    }
}

impl From<UpdateResult> for Option<MeshEvent> {
    fn from(update_result: UpdateResult) -> Option<MeshEvent> {
        match update_result {
            UpdateResult::Create { mut object } | UpdateResult::Update { mut object } => {
                object.unset_resource_version();
                Some(MeshEvent::Update { object })
            }
            UpdateResult::Delete { mut object } => {
                object.unset_resource_version();
                Some(MeshEvent::Delete { object })
            }
            UpdateResult::Snapshot { mut snapshot, .. } => {
                snapshot.iter_mut().for_each(|(_, object)| {
                    object.unset_resource_version();
                });
                Some(MeshEvent::Snapshot { snapshot })
            }
            UpdateResult::Skip | UpdateResult::Tombstone { .. } => None,
        }
    }
}
