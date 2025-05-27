use super::types::{MergeResult, MergeStrategy};
use crate::kube::dynamic_object_ext::DynamicObjectExt;
use anyhow::Result;
use kube::api::DynamicObject;

pub struct AnyApplicationMerge {}

impl AnyApplicationMerge {
    pub fn new() -> Self {
        AnyApplicationMerge {}
    }
}

impl MergeStrategy for AnyApplicationMerge {
    fn merge_update(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
    ) -> Result<MergeResult> {
        let mut incoming = incoming.clone();
        incoming.metadata.managed_fields = None;
        incoming.metadata.uid = None;
        if let Some(_current) = current {
            Ok(MergeResult::Update {
                object: incoming.clone(),
            })
        } else {
            Ok(MergeResult::Create {
                object: incoming.clone(),
            })
        }
    }

    fn merge_delete(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
    ) -> Result<MergeResult> {
        if let Some(_) = current {
            Ok(MergeResult::Delete {
                name: incoming.get_namespaced_name(),
            })
        } else {
            Ok(MergeResult::DoNothing)
        }
    }
}
