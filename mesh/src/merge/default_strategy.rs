use super::types::{MergeResult, MergeStrategy};
use crate::kube::dynamic_object_ext::DynamicObjectExt;
use anyhow::Result;
use kube::api::DynamicObject;

pub struct DefaultMerge {}

impl DefaultMerge {
    pub fn new() -> Self {
        DefaultMerge {}
    }
}

impl MergeStrategy for DefaultMerge {
    fn merge_update(
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

                Ok(MergeResult::Create { object })
            } else {
                Ok(MergeResult::DoNothing)
            }
        }
    }

    fn merge_delete(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        if let Some(current) = current {
            let current_owner_version = current.get_owner_version()?;

            let incoming_owner_version = incoming.get_owner_version()?;
            let incoming_owner_zone = incoming.get_owner_zone()?;

            let acceptable_zone = &incoming_owner_zone == incoming_zone;
            let new_version = incoming_owner_version > current_owner_version;

            if acceptable_zone && new_version {
                Ok(MergeResult::Delete {
                    name: incoming.get_namespaced_name(),
                })
            } else {
                Ok(MergeResult::DoNothing)
            }
        } else {
            Ok(MergeResult::DoNothing)
        }
    }
}

#[cfg(test)]
pub mod tests {
    use kube::api::DynamicObject;

    use super::*;
    use crate::kube::{cache::Version, dynamic_object_ext::DynamicObjectExt};

    #[test]
    pub fn non_existing_create() {
        let incoming = make_object("test", 2, "value");

        let strategy = DefaultMerge::new();
        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            strategy.merge_update(None, &incoming, &"test").unwrap()
        );
    }

    #[test]
    pub fn non_existing_other_zone() {
        let incoming = make_object("other", 2, "value");

        let strategy = DefaultMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy.merge_update(None, &incoming, &"test").unwrap()
        );
    }

    #[test]
    pub fn update_same_version() {
        let current = make_object("test", 1, "value");
        let incoming = make_object("test", 1, "value");

        let strategy = DefaultMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy
                .merge_update(Some(current), &incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn update_greater_version() {
        let current = make_object("test", 1, "value");
        let incoming = make_object("test", 2, "updated");

        let strategy = DefaultMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: incoming.to_owned()
            },
            strategy
                .merge_update(Some(current), &incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn update_other_zone() {
        let current = make_object("test", 1, "value");
        let incoming = make_object("other", 2, "updated");

        let strategy = DefaultMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy
                .merge_update(Some(current), &incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn non_existing_delete() {
        let incoming = make_object("test", 1, "value");

        let strategy = DefaultMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy.merge_delete(None, &incoming, &"test").unwrap()
        );
    }

    #[test]
    pub fn the_same_version_delete() {
        let current = make_object("test", 1, "value");
        let incoming = make_object("test", 1, "value");

        let strategy = DefaultMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy
                .merge_delete(Some(current), &incoming, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn greater_version_delete() {
        let current = make_object("test", 1, "value");
        let incoming = make_object("test", 2, "value");

        let strategy = DefaultMerge::new();
        assert_eq!(
            MergeResult::Delete {
                name: current.get_namespaced_name()
            },
            strategy
                .merge_delete(Some(current), &incoming, &"test")
                .unwrap()
        );
    }

    fn make_object(zone: &str, version: Version, data: &str) -> DynamicObject {
        let mut object: DynamicObject = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "MyKind",
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
