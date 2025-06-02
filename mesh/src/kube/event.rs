use super::dynamic_object_ext::DynamicObjectExt;
use super::types::NamespacedName;
use crate::kube::pool::Version;
use kube::api::DynamicObject;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{self, Display};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum KubeEvent {
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

impl KubeEvent {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("encoding network message");
        bytes
    }

    pub fn version(&self) -> &Version {
        match self {
            KubeEvent::Update { version, .. } => version,
            KubeEvent::Delete { version, .. } => version,
            KubeEvent::Snapshot { version, .. } => version,
        }
    }
}

impl TryFrom<Vec<u8>> for KubeEvent {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        ciborium::from_reader(&bytes[..])
    }
}

impl Display for KubeEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KubeEvent::Update { version, object } => {
                write!(f, "Update({}, {:?})", version, object.get_namespaced_name())
            }
            KubeEvent::Delete { version, object } => {
                write!(f, "Delete({}, {:?})", version, object.get_namespaced_name())
            }
            KubeEvent::Snapshot { version, snapshot } => {
                write!(f, "Snapshot({}, {} items)", version, snapshot.len())
            }
        }
    }
}
