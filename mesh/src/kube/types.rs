use kube::api::DynamicObject;
use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::sync::Arc;

use super::dynamic_object_ext::DynamicObjectExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct NamespacedName {
    pub namespace: String,
    pub name: String,
}

impl NamespacedName {
    pub fn new(namespace: String, name: String) -> Self {
        NamespacedName { namespace, name }
    }
}
// TODO explicitely keep version for each protocol item

#[derive(Debug, Clone, PartialEq)]
pub enum CacheProtocol {
    Update(DynamicObject),
    Delete(DynamicObject),
    Snapshot {
        snapshot: BTreeMap<NamespacedName, Arc<DynamicObject>>,
    },
}

impl Display for CacheProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheProtocol::Update(obj) => write!(f, "Update({:?})", obj.get_namespaced_name()),
            CacheProtocol::Delete(obj) => write!(f, "Delete({:?})", obj.get_namespaced_name()),
            CacheProtocol::Snapshot { snapshot } => write!(f, "Snapshot({} items)", snapshot.len()),
        }
    }
}
