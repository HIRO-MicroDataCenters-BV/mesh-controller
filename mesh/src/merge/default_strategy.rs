use super::types::{MergeResult, MergeStrategy, UpdateResult};
use crate::kube::{pool::Version, dynamic_object_ext::DynamicObjectExt};
use anyhow::Result;
use kube::api::{DynamicObject, GroupVersionKind, TypeMeta};

pub struct DefaultMerge {
    gvk: GroupVersionKind,
}

impl DefaultMerge {
    pub fn new(gvk: GroupVersionKind) -> Self {
        DefaultMerge { gvk }
    }
}

impl MergeStrategy for DefaultMerge {
    fn mesh_update(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let incoming = incoming.clone();
        if let Some(current) = current {
            let current_owner_version = current.get_owner_version()?;

            let incoming_owner_version = incoming.get_owner_version()?;
            let incoming_owner_zone = incoming.get_owner_zone()?;

            let acceptable_zone = &incoming_owner_zone == incoming_zone;
            let new_version = incoming_owner_version > current_owner_version;

            if acceptable_zone && new_version {
                let mut object = incoming.clone();
                object.metadata.managed_fields = None;
                object.metadata.uid = None;
                object.types = Some(TypeMeta {
                    api_version: self.gvk.api_version(),
                    kind: self.gvk.kind.to_owned(),
                });
                Ok(MergeResult::Update { object })
            } else {
                Ok(MergeResult::DoNothing)
            }
        } else {
            let incoming_owner_zone = incoming.get_owner_zone()?;
            let acceptable_zone = &incoming_owner_zone == incoming_zone;
            if acceptable_zone {
                let mut object = incoming.clone();
                object.metadata.managed_fields = None;
                object.metadata.uid = None;
                object.types = Some(TypeMeta {
                    api_version: self.gvk.api_version(),
                    kind: self.gvk.kind.to_owned(),
                });
                Ok(MergeResult::Create { object })
            } else {
                Ok(MergeResult::DoNothing)
            }
        }
    }

    fn mesh_delete(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        if let Some(_current) = current {
            let incoming_owner_zone = incoming.get_owner_zone()?;

            let acceptable_zone = &incoming_owner_zone == incoming_zone;

            if acceptable_zone {
                Ok(MergeResult::Delete {
                    gvk: self.gvk.to_owned(),
                    name: incoming.get_namespaced_name(),
                })
            } else {
                Ok(MergeResult::DoNothing)
            }
        } else {
            Ok(MergeResult::DoNothing)
        }
    }

    fn local_update(
        &self,
        current: Option<DynamicObject>,
        mut incoming: DynamicObject,
        incoming_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult> {
        incoming.normalize(incoming_zone);
        let is_current_zone = incoming.get_owner_zone()? == incoming_zone;
        if !is_current_zone {
            return Ok(UpdateResult::DoNothing);
        }
        if let Some(curr) = current {
            if curr.get_owner_version()? >= incoming_version {
                return Ok(UpdateResult::DoNothing);
            }

            incoming.set_owner_version(incoming_version);
            return Ok(UpdateResult::Update { object: incoming });
        } else {
            incoming.set_owner_version(incoming_version);
            return Ok(UpdateResult::Create { object: incoming });
        }
    }

    fn local_delete(
        &self,
        current: Option<DynamicObject>,
        mut incoming: DynamicObject,
        incoming_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult> {
        incoming.normalize(incoming_zone);
        let is_current_zone = incoming.get_owner_zone()? == incoming_zone;
        if !is_current_zone {
            return Ok(UpdateResult::DoNothing);
        }
        if current.is_some() {
            incoming.set_owner_version(incoming_version);
            return Ok(UpdateResult::Delete { object: incoming });
        } else {
            return Ok(UpdateResult::DoNothing);
        }
    }

    fn is_owner_zone(&self, current: &DynamicObject, zone: &str) -> bool {
        current.get_owner_zone().unwrap_or(zone.into()) == zone
    }
}

#[cfg(test)]
pub mod tests {
    use kube::api::DynamicObject;

    use super::*;
    use crate::kube::{pool::Version, dynamic_object_ext::DynamicObjectExt};

    #[test]
    pub fn non_existing_create() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");

        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .mesh_update(None, &incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn non_existing_other_zone() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("other", 2, "value");

        assert_eq!(
            MergeResult::DoNothing,
            DefaultMerge::new(gvk)
                .mesh_update(None, &incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn update_same_version() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let current = make_object("test", 1, "value");
        let incoming = make_object("test", 1, "value");

        assert_eq!(
            MergeResult::DoNothing,
            DefaultMerge::new(gvk)
                .mesh_update(Some(current), &incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn update_greater_version() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let current = make_object("test", 1, "value");
        let incoming = make_object("test", 2, "updated");

        assert_eq!(
            MergeResult::Update {
                object: incoming.to_owned()
            },
            DefaultMerge::new(gvk)
                .mesh_update(Some(current), &incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn update_other_zone() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let current = make_object("test", 1, "value");
        let incoming = make_object("other", 2, "updated");

        assert_eq!(
            MergeResult::DoNothing,
            DefaultMerge::new(gvk)
                .mesh_update(Some(current), &incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn non_existing_delete() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 1, "value");

        assert_eq!(
            MergeResult::DoNothing,
            DefaultMerge::new(gvk)
                .mesh_delete(None, &incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn the_same_version_delete() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let current = make_object("test", 1, "value");
        let incoming = make_object("test", 1, "value");

        assert_eq!(
            MergeResult::Delete {
                gvk: gvk.to_owned(),
                name: incoming.get_namespaced_name()
            },
            DefaultMerge::new(gvk)
                .mesh_delete(Some(current), &incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn greater_version_delete() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let current = make_object("test", 1, "value");
        let incoming = make_object("test", 2, "value");

        assert_eq!(
            MergeResult::Delete {
                gvk: incoming.get_gvk().unwrap(),
                name: current.get_namespaced_name()
            },
            DefaultMerge::new(gvk)
                .mesh_delete(Some(current), &incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_create() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");

        assert_eq!(
            UpdateResult::Create {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .local_update(None, incoming, 2, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_skip() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 1, "value2");
        let existing = make_object("test", 1, "value1");

        assert_eq!(
            UpdateResult::DoNothing,
            DefaultMerge::new(gvk)
                .local_update(Some(existing), incoming, 1, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value2");
        let existing = make_object("test", 1, "value1");

        assert_eq!(
            UpdateResult::Update {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .local_update(Some(existing), incoming, 2, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_skip() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");

        assert_eq!(
            UpdateResult::DoNothing,
            DefaultMerge::new(gvk)
                .local_delete(None, incoming, 2, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete() {
        let gvk = GroupVersionKind::gvk("", "v1", "Secret");
        let incoming = make_object("test", 2, "value");

        assert_eq!(
            UpdateResult::Create {
                object: incoming.clone()
            },
            DefaultMerge::new(gvk)
                .local_update(None, incoming, 2, &"test")
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
