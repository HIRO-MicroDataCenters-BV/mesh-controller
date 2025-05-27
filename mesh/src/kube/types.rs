use super::dynamic_object_ext::DynamicObjectExt;
use crate::kube::cache::Version;
use kube::api::DynamicObject;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NamespacedName {
    pub namespace: String,
    pub name: String,
}

impl NamespacedName {
    pub fn new(namespace: String, name: String) -> Self {
        NamespacedName { namespace, name }
    }
}

impl Display for NamespacedName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CacheProtocol {
    Update {
        version: Version,
        zone: String,
        object: DynamicObject,
    },
    Delete {
        version: Version,
        zone: String,
        object: DynamicObject,
    },
    Snapshot {
        version: Version,
        zone: String,
        snapshot: BTreeMap<NamespacedName, Arc<DynamicObject>>,
    },
}

impl CacheProtocol {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("encoding network message");
        bytes
    }

    pub fn version(&self) -> &Version {
        match self {
            CacheProtocol::Update { version, .. } => version,
            CacheProtocol::Delete { version, .. } => version,
            CacheProtocol::Snapshot { version, .. } => version,
        }
    }
}

impl TryFrom<Vec<u8>> for CacheProtocol {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        ciborium::from_reader(&bytes[..])
    }
}

impl Display for CacheProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheProtocol::Update {
                zone,
                version,
                object,
            } => {
                write!(
                    f,
                    "Update({}, {}, {:?})",
                    zone,
                    version,
                    object.get_namespaced_name()
                )
            }
            CacheProtocol::Delete {
                zone,
                version,
                object,
            } => {
                write!(
                    f,
                    "Delete({}, {}, {:?})",
                    zone,
                    version,
                    object.get_namespaced_name()
                )
            }
            CacheProtocol::Snapshot {
                zone,
                version,
                snapshot,
            } => {
                write!(
                    f,
                    "Snapshot({}, {}, {} items)",
                    zone,
                    version,
                    snapshot.len()
                )
            }
        }
    }
}
