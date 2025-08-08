use super::types::{MergeResult, MergeStrategy, UpdateResult};
use crate::{
    kube::{dynamic_object_ext::DynamicObjectExt, subscriptions::Version},
    merge::types::{Tombstone, VersionedObject},
    mesh::event::MeshEvent,
    network::discovery::types::Membership,
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
        _membership: &Membership,
    ) -> Result<MergeResult> {
        match current {
            VersionedObject::Object(current) => {
                self.mesh_update_update(current, incoming, incoming_zone)
            }
            VersionedObject::NonExisting => self.mesh_update_create(incoming, incoming_zone),
            VersionedObject::Tombstone(tombstone) => {
                self.mesh_update_tombstone(tombstone.owner_version, incoming, incoming_zone)
            }
        }
    }

    fn mesh_delete(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_zone: &str,
        now_millis: u64,
    ) -> Result<MergeResult> {
        let incoming_owner_zone = incoming.get_owner_zone()?;
        let incoming_owner_version = incoming.get_owner_version_or_fail()?;
        let acceptable_zone = incoming_owner_zone == incoming_zone;
        if !acceptable_zone {
            return Ok(MergeResult::Skip);
        }

        match current {
            VersionedObject::Object(current) => Ok(MergeResult::Delete(Tombstone {
                gvk: self.gvk.to_owned(),
                name: incoming.get_namespaced_name(),
                owner_version: incoming_owner_version,
                owner_zone: incoming_owner_zone,
                resource_version: current.get_resource_version(),
                deletion_timestamp: now_millis,
            })),
            VersionedObject::NonExisting => Ok(MergeResult::Tombstone(Tombstone {
                gvk: self.gvk.to_owned(),
                name: incoming.get_namespaced_name(),
                owner_version: incoming_owner_version,
                owner_zone: incoming_owner_zone,
                resource_version: 0,
                deletion_timestamp: now_millis,
            })),
            VersionedObject::Tombstone(tombstone) => {
                if tombstone.owner_zone == incoming_zone {
                    let version = Version::max(tombstone.owner_version, incoming_owner_version);
                    Ok(MergeResult::Tombstone(Tombstone {
                        gvk: self.gvk.to_owned(),
                        name: incoming.get_namespaced_name(),
                        owner_version: version,
                        owner_zone: incoming_owner_zone,
                        resource_version: tombstone.resource_version,
                        deletion_timestamp: tombstone.deletion_timestamp,
                    }))
                } else {
                    Ok(MergeResult::Skip)
                }
            }
        }
    }

    fn kube_update(
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
            VersionedObject::Tombstone(tombstone) => {
                if tombstone.owner_version >= incoming_resource_version {
                    return Ok(UpdateResult::Skip);
                }
                incoming.set_owner_version(incoming_resource_version);
                incoming.set_owner_zone(incoming_zone.into());
                Ok(UpdateResult::Create { object: incoming })
            }
        }
    }

    fn kube_delete(
        &self,
        current: VersionedObject,
        mut incoming: DynamicObject,
        incoming_version: Version,
        incoming_zone: &str,
        now_millis: u64,
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
                    tombstone: Tombstone {
                        gvk: self.gvk.to_owned(),
                        name,
                        owner_version: incoming_version,
                        owner_zone: incoming_zone.into(),
                        resource_version: incoming_version,
                        deletion_timestamp: now_millis,
                    },
                    object: incoming,
                })
            }
            VersionedObject::NonExisting => Ok(UpdateResult::Tombstone(Tombstone {
                gvk: self.gvk.to_owned(),
                name,
                owner_version: incoming_version,
                owner_zone: incoming_zone.into(),
                resource_version: incoming_version,
                deletion_timestamp: now_millis,
            })),
            VersionedObject::Tombstone(tombstone) => {
                let max_version = Version::max(tombstone.owner_version, incoming_version);
                Ok(UpdateResult::Tombstone(Tombstone {
                    gvk: self.gvk.to_owned(),
                    name,
                    owner_version: max_version,
                    owner_zone: incoming_zone.to_owned(),
                    resource_version: tombstone.resource_version,
                    deletion_timestamp: now_millis,
                }))
            }
        }
    }

    fn is_owner_zone(&self, current: &VersionedObject, zone: &str) -> bool {
        match current {
            VersionedObject::Object(current) => self.is_owner_zone_object(current, zone),
            VersionedObject::NonExisting => false,
            VersionedObject::Tombstone(tombstone) => tombstone.owner_zone == zone,
        }
    }

    fn is_owner_zone_object(&self, current: &DynamicObject, zone: &str) -> bool {
        current.get_owner_zone().unwrap_or(zone.into()) == zone
    }

    fn tombstone(&self, current: VersionedObject, now_millis: u64) -> Result<Option<Tombstone>> {
        match current {
            VersionedObject::Object(current) => {
                let owner_zone = current.get_owner_zone()?;
                let owner_version = current.get_owner_version().unwrap_or(0);
                let tombstone = Tombstone {
                    gvk: self.gvk.to_owned(),
                    name: current.get_namespaced_name(),
                    owner_version,
                    owner_zone: owner_zone.to_owned(),
                    resource_version: current.get_resource_version(),
                    deletion_timestamp: now_millis,
                };
                Ok(Some(tombstone))
            }
            VersionedObject::NonExisting => Ok(None),
            VersionedObject::Tombstone(tombstone) => Ok(Some(tombstone.clone())),
        }
    }

    fn mesh_membership_change(
        &self,
        _current: VersionedObject,
        _membership: &Membership,
        _node_zone: &str,
    ) -> Result<Vec<MeshEvent>> {
        Ok(vec![])
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
        let current_owner_version = current.get_owner_version_or_fail()?;

        let incoming_owner_version = incoming.get_owner_version_or_fail()?;
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
            Ok(MergeResult::Update {
                object,
                event: None,
            })
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
        let incoming_owner_version = incoming.get_owner_version_or_fail()?;
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
        let membership = Membership::default();
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");

        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .mesh_update(
                    VersionedObject::NonExisting,
                    incoming,
                    "test",
                    "test",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_tombstone_create() {
        let membership = Membership::default();
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: gvk.to_owned(),
            name: incoming.get_namespaced_name(),
            owner_version: 1,
            owner_zone: "test".into(),
            resource_version: 0,
            deletion_timestamp: 0,
        });

        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .mesh_update(existing, incoming, "test", "test", &membership)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_tombstone_skip_create_if_obsolete() {
        let membership = Membership::default();
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: gvk.to_owned(),
            name: incoming.get_namespaced_name(),
            owner_version: 3,
            owner_zone: "test".into(),
            resource_version: 0,
            deletion_timestamp: 0,
        });

        assert_eq!(
            MergeResult::Skip,
            DefaultMerge::new(gvk)
                .mesh_update(existing, incoming, "test", "test", &membership)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_non_existing_other_zone() {
        let membership = Membership::default();
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("other", 2, "value");

        assert_eq!(
            MergeResult::Skip,
            DefaultMerge::new(gvk)
                .mesh_update(
                    VersionedObject::NonExisting,
                    incoming,
                    "test",
                    "test",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_versions_equal() {
        let membership = Membership::default();
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let current = make_object("test", 1, "value");
        let incoming = make_object("test", 1, "value");

        assert_eq!(
            MergeResult::Skip,
            DefaultMerge::new(gvk)
                .mesh_update(current.into(), incoming, "test", "test", &membership)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_incoming_version_greater() {
        let membership = Membership::default();
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let current = make_object("test", 1, "value");
        let incoming = make_object("test", 2, "updated");

        assert_eq!(
            MergeResult::Update {
                object: incoming.to_owned(),
                event: None,
            },
            DefaultMerge::new(gvk)
                .mesh_update(current.into(), incoming, "test", "test", &membership)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_other_zone() {
        let membership = Membership::default();
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let current = make_object("test", 1, "value");
        let incoming = make_object("other", 2, "updated");

        assert_eq!(
            MergeResult::Skip,
            DefaultMerge::new(gvk)
                .mesh_update(current.into(), incoming, "test", "test", &membership)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_non_existing() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 1, "value");

        assert_eq!(
            MergeResult::Tombstone(Tombstone {
                gvk: gvk.to_owned(),
                name: incoming.get_namespaced_name(),
                owner_version: 1,
                owner_zone: "test".into(),
                resource_version: 0,
                deletion_timestamp: 17
            }),
            DefaultMerge::new(gvk)
                .mesh_delete(VersionedObject::NonExisting, incoming, "test", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_tombstone_skip_if_already_deleted() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: gvk.to_owned(),
            name: incoming.get_namespaced_name(),
            owner_version: 1,
            owner_zone: "test".into(),
            resource_version: 5,
            deletion_timestamp: 0,
        });

        assert_eq!(
            MergeResult::Tombstone(Tombstone {
                gvk: gvk.to_owned(),
                name: incoming.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "test".into(),
                resource_version: 5,
                deletion_timestamp: 0,
            }),
            DefaultMerge::new(gvk)
                .mesh_delete(existing, incoming, "test", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_tombstone_skip_if_obsolete() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: gvk.to_owned(),
            name: incoming.get_namespaced_name(),
            owner_version: 3,
            owner_zone: "test".into(),
            resource_version: 5,
            deletion_timestamp: 0,
        });

        assert_eq!(
            MergeResult::Tombstone(Tombstone {
                gvk: gvk.to_owned(),
                name: incoming.get_namespaced_name(),
                owner_version: 3,
                owner_zone: "test".into(),
                resource_version: 5,
                deletion_timestamp: 0,
            }),
            DefaultMerge::new(gvk)
                .mesh_delete(existing, incoming, "test", 17)
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
            MergeResult::Delete(Tombstone {
                gvk: gvk.to_owned(),
                name: incoming.get_namespaced_name(),
                owner_version: 1,
                owner_zone: "test".into(),
                resource_version: 10,
                deletion_timestamp: 17,
            }),
            DefaultMerge::new(gvk)
                .mesh_delete(current.into(), incoming, "test", 17)
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
            MergeResult::Delete(Tombstone {
                gvk: incoming.get_gvk().unwrap(),
                name: current.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "test".into(),
                resource_version: 10,
                deletion_timestamp: 17,
            }),
            DefaultMerge::new(gvk)
                .mesh_delete(current.into(), incoming, "test", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn kube_update_create_non_existing() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");

        assert_eq!(
            UpdateResult::Create {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .kube_update(VersionedObject::NonExisting, incoming, 2, "test")
                .unwrap()
        );
    }

    #[test]
    pub fn kube_update_create_tombstone_is_old() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");

        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: gvk.to_owned(),
            name: incoming.get_namespaced_name(),
            owner_version: 1,
            owner_zone: "test".into(),
            resource_version: 5,
            deletion_timestamp: 0,
        });

        assert_eq!(
            UpdateResult::Create {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .kube_update(existing, incoming, 2, "test")
                .unwrap()
        );
    }

    #[test]
    pub fn kube_update_skip_versions_equal() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 1, "value2");
        let mut existing = make_object("test", 1, "value1");
        existing.set_resource_version(1);

        assert_eq!(
            UpdateResult::Skip,
            DefaultMerge::new(gvk)
                .kube_update(existing.into(), incoming, 1, "test")
                .unwrap()
        );
    }

    #[test]
    pub fn kube_update_skip_event_with_delete_timestamp() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let mut incoming = make_object("test", 2, "value2");
        let existing = make_object("test", 1, "value1");

        incoming.metadata.deletion_timestamp = Some(Time(SystemTime::now().into()));

        assert_eq!(
            UpdateResult::Skip,
            DefaultMerge::new(gvk)
                .kube_update(existing.into(), incoming, 2, "test")
                .unwrap()
        );
    }

    #[test]
    pub fn kube_update_incoming_version_is_greater() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value2");
        let mut existing = make_object("test", 1, "value1");
        existing.set_resource_version(1);

        assert_eq!(
            UpdateResult::Update {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .kube_update(existing.into(), incoming, 2, "test")
                .unwrap()
        );
    }

    #[test]
    pub fn kube_delete_skip_non_existing() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");

        assert_eq!(
            UpdateResult::Tombstone(Tombstone {
                gvk: gvk.to_owned(),
                name: incoming.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "test".into(),
                resource_version: 2,
                deletion_timestamp: 17,
            }),
            DefaultMerge::new(gvk)
                .kube_delete(VersionedObject::NonExisting, incoming, 2, "test", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn kube_delete_skip_if_tombstone() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: gvk.to_owned(),
            name: incoming.get_namespaced_name(),
            owner_version: 1,
            owner_zone: "test".into(),
            resource_version: 5,
            deletion_timestamp: 0,
        });

        assert_eq!(
            UpdateResult::Tombstone(Tombstone {
                gvk: gvk.to_owned(),
                name: incoming.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "test".into(),
                resource_version: 5,
                deletion_timestamp: 17,
            }),
            DefaultMerge::new(gvk)
                .kube_delete(existing, incoming, 2, "test", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn kube_delete_incoming_version_greater() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");
        let mut existing = make_object("test", 1, "value");
        existing.set_resource_version(1);

        assert_eq!(
            UpdateResult::Delete {
                object: incoming.to_owned(),
                tombstone: Tombstone {
                    gvk: gvk.to_owned(),
                    name: incoming.get_namespaced_name(),
                    owner_version: 2,
                    owner_zone: "test".into(),
                    resource_version: 2,
                    deletion_timestamp: 17,
                }
            },
            DefaultMerge::new(gvk)
                .kube_delete(existing.into(), incoming, 2, "test", 17)
                .unwrap()
        );
    }

    fn make_object(zone: &str, version: Version, data: &str) -> DynamicObject {
        let mut object: DynamicObject = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "example",
                "namespace": "default"
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
