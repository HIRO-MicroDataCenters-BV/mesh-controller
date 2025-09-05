use std::{collections::BTreeMap, fmt::Display};

use kube::api::DynamicObject;
use meshkube::kube::{
    dynamic_object_ext::DynamicObjectExt, subscriptions::Version, types::NamespacedName,
};
use serde::{Deserialize, Serialize};

use crate::merge::types::UpdateResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MeshEvent {
    Update {
        version: Version,
        object: DynamicObject,
    },
    Delete {
        version: Version,
        object: DynamicObject,
    },
    Snapshot {
        version: Version,
        snapshot: BTreeMap<NamespacedName, DynamicObject>,
    },
}

impl MeshEvent {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("encoding network message");
        bytes
    }

    pub fn set_zone_version(&mut self, new_version: Version) {
        let version = match self {
            MeshEvent::Update { version, .. } => version,
            MeshEvent::Delete { version, .. } => version,
            MeshEvent::Snapshot { version, .. } => version,
        };
        *version = new_version;
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
            MeshEvent::Update { object, .. } => {
                write!(f, "Update({:?})", object.get_namespaced_name())
            }
            MeshEvent::Delete { object, .. } => {
                write!(f, "Delete({:?})", object.get_namespaced_name())
            }
            MeshEvent::Snapshot { snapshot, version } => {
                write!(f, "Snapshot({} items, version{})", snapshot.len(), version)
            }
        }
    }
}

impl From<UpdateResult> for Option<MeshEvent> {
    fn from(update_result: UpdateResult) -> Option<MeshEvent> {
        match update_result {
            UpdateResult::Create {
                mut object,
                version,
            }
            | UpdateResult::Update {
                mut object,
                version,
            } => {
                object.unset_resource_version();
                Some(MeshEvent::Update { object, version })
            }
            UpdateResult::Delete {
                mut object,
                version,
                ..
            } => {
                object.unset_resource_version();
                Some(MeshEvent::Delete { object, version })
            }
            UpdateResult::Snapshot {
                mut snapshot,
                version,
            } => {
                snapshot.iter_mut().for_each(|(_, object)| {
                    object.unset_resource_version();
                });
                Some(MeshEvent::Snapshot { snapshot, version })
            }
            UpdateResult::Skip | UpdateResult::Tombstone { .. } => None,
        }
    }
}
