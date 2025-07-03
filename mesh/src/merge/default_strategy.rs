use super::types::{MergeResult, MergeStrategy, UpdateResult};
use crate::{
    kube::{dynamic_object_ext::DynamicObjectExt, subscriptions::Version},
    merge::types::VersionedObject,
};
use anyhow::Result;
use kube::api::{DynamicObject, GroupVersionKind, TypeMeta};

pub struct DefaultMerge {
    gvk: GroupVersionKind,
}

impl MergeStrategy for DefaultMerge {
    fn mesh_update(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_zone: &str,
        _current_zone: &str,
    ) -> Result<MergeResult> {
        match current {
            VersionedObject::Object(current) => {
                self.mesh_update_update(current, incoming, incoming_zone)
            }
            VersionedObject::NonExisting => self.mesh_update_create(incoming, incoming_zone),
            VersionedObject::Tombstone(current_owner_version, ..) => {
                self.mesh_update_tombstone(current_owner_version, incoming, incoming_zone)
            }
        }
    }

    fn mesh_delete(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let incoming_owner_zone = incoming.get_owner_zone()?;
        let incoming_owner_version = incoming.get_owner_version()?;
        let acceptable_zone = incoming_owner_zone == incoming_zone;
        if !acceptable_zone {
            return Ok(MergeResult::Skip);
        }

        match current {
            VersionedObject::Object(current) => Ok(MergeResult::Delete {
                gvk: self.gvk.to_owned(),
                name: incoming.get_namespaced_name(),
                owner_version: incoming_owner_version,
                owner_zone: incoming_owner_zone,
                resource_version: current.get_resource_version(),
            }),
            VersionedObject::NonExisting => Ok(MergeResult::Tombstone {
                name: incoming.get_namespaced_name(),
                owner_version: incoming_owner_version,
                owner_zone: incoming_owner_zone,
            }),
            VersionedObject::Tombstone(owner_version, owner_zone) => {
                if owner_zone == incoming_zone {
                    let version = Version::max(owner_version, incoming_owner_version);
                    Ok(MergeResult::Tombstone {
                        name: incoming.get_namespaced_name(),
                        owner_version: version,
                        owner_zone: incoming_owner_zone,
                    })
                } else {
                    Ok(MergeResult::Skip)
                }
            }
        }
    }

    fn local_update(
        &self,
        current: VersionedObject,
        mut incoming: DynamicObject,
        incoming_resource_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult> {
        if incoming.metadata.deletion_timestamp.is_some() {
            return Ok(UpdateResult::Skip);
        }

        incoming.normalize(incoming_zone);
        let is_current_zone = incoming.get_owner_zone()? == incoming_zone;
        if !is_current_zone {
            return Ok(UpdateResult::Skip);
        }

        match current {
            VersionedObject::Object(current) => {
                let current_resource_version = current.get_resource_version();
                if current_resource_version >= incoming_resource_version {
                    return Ok(UpdateResult::Skip);
                }
                incoming.set_owner_version(incoming_resource_version);
                incoming.set_owner_zone(incoming_zone.into());
                Ok(UpdateResult::Update { object: incoming })
            }
            VersionedObject::NonExisting => {
                incoming.set_owner_version(incoming_resource_version);
                incoming.set_owner_zone(incoming_zone.into());
                Ok(UpdateResult::Create { object: incoming })
            }
            VersionedObject::Tombstone(current_owner_version, _) => {
                if current_owner_version >= incoming_resource_version {
                    return Ok(UpdateResult::Skip);
                }
                incoming.set_owner_version(incoming_resource_version);
                incoming.set_owner_zone(incoming_zone.into());
                Ok(UpdateResult::Create { object: incoming })
            }
        }
    }

    fn local_delete(
        &self,
        current: VersionedObject,
        mut incoming: DynamicObject,
        incoming_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult> {
        incoming.normalize(incoming_zone);
        let name = incoming.get_namespaced_name();
        let is_current_zone = incoming.get_owner_zone()? == incoming_zone;
        if !is_current_zone {
            return Ok(UpdateResult::Skip);
        }
        match current {
            VersionedObject::Object(current) => {
                let current_resource_version = current.get_resource_version();
                if current_resource_version >= incoming_version {
                    return Ok(UpdateResult::Skip);
                }
                incoming.set_owner_version(incoming_version);
                incoming.set_owner_zone(incoming_zone.into());
                Ok(UpdateResult::Delete {
                    object: incoming,
                    owner_version: incoming_version,
                    owner_zone: incoming_zone.into(),
                })
            }
            VersionedObject::NonExisting => Ok(UpdateResult::Tombstone {
                name,
                owner_version: incoming_version,
                owner_zone: incoming_zone.to_owned(),
            }),
            VersionedObject::Tombstone(owner_version, _owner_zone) => {
                let max_version = Version::max(owner_version, incoming_version);
                Ok(UpdateResult::Tombstone {
                    name,
                    owner_version: max_version,
                    owner_zone: incoming_zone.to_owned(),
                })
            }
        }
    }

    fn is_owner_zone(&self, current: &VersionedObject, zone: &str) -> bool {
        match current {
            VersionedObject::Object(current) => self.is_owner_zone_object(current, zone),
            VersionedObject::NonExisting => false,
            VersionedObject::Tombstone(_, owner_zone) => owner_zone == zone,
        }
    }

    fn is_owner_zone_object(&self, current: &DynamicObject, zone: &str) -> bool {
        current.get_owner_zone().unwrap_or(zone.into()) == zone
    }
}

impl DefaultMerge {
    pub fn new(gvk: GroupVersionKind) -> Self {
        DefaultMerge { gvk }
    }

    fn mesh_update_update(
        &self,
        current: DynamicObject,
        incoming: DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let current_owner_version = current.get_owner_version()?;

        let incoming_owner_version = incoming.get_owner_version()?;
        let incoming_owner_zone = incoming.get_owner_zone()?;

        let acceptable_zone = incoming_owner_zone == incoming_zone;
        let new_version = incoming_owner_version > current_owner_version;

        if acceptable_zone && new_version {
            let mut object = incoming.clone();
            object.metadata.managed_fields = None;
            object.metadata.uid = None;
            object.metadata.resource_version = current.metadata.resource_version;
            object.types = Some(TypeMeta {
                api_version: self.gvk.api_version(),
                kind: self.gvk.kind.to_owned(),
            });
            Ok(MergeResult::Update { object })
        } else {
            Ok(MergeResult::Skip)
        }
    }

    fn mesh_update_create(
        &self,
        incoming: DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let incoming_owner_zone = incoming.get_owner_zone()?;
        let acceptable_zone = incoming_owner_zone == incoming_zone;
        if acceptable_zone {
            let mut object = incoming;
            object.metadata.managed_fields = None;
            object.metadata.uid = None;
            object.types = Some(TypeMeta {
                api_version: self.gvk.api_version(),
                kind: self.gvk.kind.to_owned(),
            });
            Ok(MergeResult::Create { object })
        } else {
            Ok(MergeResult::Skip)
        }
    }

    fn mesh_update_tombstone(
        &self,
        current_owner_version: Version,
        incoming: DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let incoming_owner_version = incoming.get_owner_version()?;
        let incoming_owner_zone = incoming.get_owner_zone()?;
        let acceptable_zone = incoming_owner_zone == incoming_zone;

        if !acceptable_zone {
            return Ok(MergeResult::Skip);
        }

        if current_owner_version >= incoming_owner_version {
            Ok(MergeResult::Skip)
        } else {
            self.mesh_update_create(incoming, incoming_zone)
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::time::SystemTime;

    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    use kube::api::DynamicObject;

    use super::*;
    use crate::kube::{dynamic_object_ext::DynamicObjectExt, subscriptions::Version};

    #[test]
    pub fn mesh_update_non_existing_create() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");

        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .mesh_update(VersionedObject::NonExisting, incoming, &"test", &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_tombstone_create() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let existing = VersionedObject::Tombstone(1, "test".into());

        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .mesh_update(existing, incoming, &"test", &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_tombstone_skip_create_if_obsolete() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let existing = VersionedObject::Tombstone(3, "test".into());

        assert_eq!(
            MergeResult::Skip,
            DefaultMerge::new(gvk)
                .mesh_update(existing, incoming, &"test", &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_non_existing_other_zone() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("other", 2, "value");

        assert_eq!(
            MergeResult::Skip,
            DefaultMerge::new(gvk)
                .mesh_update(VersionedObject::NonExisting, incoming, &"test", &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_versions_equal() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let current = make_object("test", 1, "value");
        let incoming = make_object("test", 1, "value");

        assert_eq!(
            MergeResult::Skip,
            DefaultMerge::new(gvk)
                .mesh_update(current.into(), incoming, &"test", &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_incoming_version_greater() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let current = make_object("test", 1, "value");
        let incoming = make_object("test", 2, "updated");

        assert_eq!(
            MergeResult::Update {
                object: incoming.to_owned()
            },
            DefaultMerge::new(gvk)
                .mesh_update(current.into(), incoming, &"test", &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_other_zone() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let current = make_object("test", 1, "value");
        let incoming = make_object("other", 2, "updated");

        assert_eq!(
            MergeResult::Skip,
            DefaultMerge::new(gvk)
                .mesh_update(current.into(), incoming, &"test", &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_non_existing() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 1, "value");

        assert_eq!(
            MergeResult::Tombstone {
                name: incoming.get_namespaced_name(),
                owner_version: 1,
                owner_zone: "test".into()
            },
            DefaultMerge::new(gvk)
                .mesh_delete(VersionedObject::NonExisting, incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_tombstone_skip_if_already_deleted() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let existing = VersionedObject::Tombstone(1, "test".into());

        assert_eq!(
            MergeResult::Tombstone {
                name: incoming.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "test".into()
            },
            DefaultMerge::new(gvk)
                .mesh_delete(existing, incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_tombstone_skip_if_obsolete() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let existing = VersionedObject::Tombstone(3, "test".into());

        assert_eq!(
            MergeResult::Tombstone {
                name: incoming.get_namespaced_name(),
                owner_version: 3,
                owner_zone: "test".into()
            },
            DefaultMerge::new(gvk)
                .mesh_delete(existing, incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_versions_equal() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 1, "value");
        let mut current = make_object("test", 1, "value");
        current.set_resource_version(10);

        assert_eq!(
            MergeResult::Delete {
                gvk: gvk.to_owned(),
                name: incoming.get_namespaced_name(),
                owner_version: 1,
                owner_zone: "test".into(),
                resource_version: 10,
            },
            DefaultMerge::new(gvk)
                .mesh_delete(current.into(), incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_incoming_version_greater() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let mut current = make_object("test", 1, "value");
        current.set_resource_version(10);

        assert_eq!(
            MergeResult::Delete {
                gvk: incoming.get_gvk().unwrap(),
                name: current.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "test".into(),
                resource_version: 10,
            },
            DefaultMerge::new(gvk)
                .mesh_delete(current.into(), incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_create_non_existing() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");

        assert_eq!(
            UpdateResult::Create {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .local_update(VersionedObject::NonExisting, incoming, 2, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_create_tombstone_is_old() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let existing = VersionedObject::Tombstone(1, "test".into());

        assert_eq!(
            UpdateResult::Create {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .local_update(existing, incoming, 2, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_skip_versions_equal() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 1, "value2");
        let mut existing = make_object("test", 1, "value1");
        existing.set_resource_version(1);

        assert_eq!(
            UpdateResult::Skip,
            DefaultMerge::new(gvk)
                .local_update(existing.into(), incoming, 1, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_skip_event_with_delete_timestamp() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let mut incoming = make_object("test", 2, "value2");
        let existing = make_object("test", 1, "value1");

        incoming.metadata.deletion_timestamp = Some(Time(SystemTime::now().into()));

        assert_eq!(
            UpdateResult::Skip,
            DefaultMerge::new(gvk)
                .local_update(existing.into(), incoming, 2, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_incoming_version_is_greater() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value2");
        let mut existing = make_object("test", 1, "value1");
        existing.set_resource_version(1);

        assert_eq!(
            UpdateResult::Update {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .local_update(existing.into(), incoming, 2, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_skip_non_existing() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");

        assert_eq!(
            UpdateResult::Tombstone {
                name: incoming.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "test".into()
            },
            DefaultMerge::new(gvk)
                .local_delete(VersionedObject::NonExisting, incoming, 2, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_skip_if_tombstone() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let existing = VersionedObject::Tombstone(1, "test".into());

        assert_eq!(
            UpdateResult::Tombstone {
                name: incoming.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "test".into()
            },
            DefaultMerge::new(gvk)
                .local_delete(existing, incoming, 2, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_incoming_version_greater() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let mut existing = make_object("test", 1, "value");
        existing.set_resource_version(1);

        assert_eq!(
            UpdateResult::Delete {
                object: incoming.to_owned(),
                owner_version: 2,
                owner_zone: "test".into()
            },
            DefaultMerge::new(gvk)
                .local_delete(existing.into(), incoming, 2, &"test")
                .unwrap()
        );
    }

    fn make_object(zone: &str, version: Version, data: &str) -> DynamicObject {
        let mut object: DynamicObject = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "example"
            },
            "spec": {
                "data": data,
            }
        }))
        .unwrap();
        object.set_owner_zone(zone.into());
        object.set_owner_version(version);
        object
    }
}
