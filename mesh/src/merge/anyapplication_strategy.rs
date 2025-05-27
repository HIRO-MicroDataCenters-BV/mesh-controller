use super::{
    anyapplication::{AnyApplication, AnyApplicationSpec, AnyApplicationStatus},
    types::{MergeResult, MergeStrategy},
};
use crate::kube::dynamic_object_ext::DynamicObjectExt;
use anyhow::{Context, Result, anyhow};
use kube::api::DynamicObject;
use serde::de::DeserializeOwned;

pub struct AnyApplicationMerge {}

impl AnyApplicationMerge {
    pub fn new() -> Self {
        AnyApplicationMerge {}
    }

    fn merge_spec(
        &self,
        target: &mut AnyApplicationSpec,
        from: &AnyApplicationSpec,
        from_zone: &str,
    ) -> bool {
        // This strategy does not handle status updates
        unimplemented!()
    }

    fn merge_status(
        &self,
        target: &mut Option<AnyApplicationStatus>,
        from: &Option<AnyApplicationStatus>,
        from_zone: &str,
    ) -> bool {
        // This strategy does not handle status updates
        unimplemented!()
    }
}

impl MergeStrategy for AnyApplicationMerge {
    fn merge_update(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &String,
    ) -> Result<MergeResult> {
        let incoming = incoming.clone();
        if let Some(current) = current {
            let mut into: AnyApplication = current.try_parse()?;
            let from: AnyApplication = incoming.try_parse()?;
            
            let is_spec_merged = self.merge_spec(&mut into.spec, &from.spec, incoming_zone);
            let is_status_merged = self.merge_status(&mut into.status, &from.status, incoming_zone);
            if !is_spec_merged && !is_status_merged {
                return Ok(MergeResult::DoNothing);
            }
            let value = serde_json::to_value(into).context("Failed to serialize merged object")?;
            let object: DynamicObject = serde_json::from_value(value)?;
            Ok(MergeResult::Update { object })
        } else {
            let mut target: AnyApplication = incoming.try_parse()?;
            target.metadata.managed_fields = None;
            target.metadata.uid = None;

            let owner = target.status.as_ref().map(|s|s.owner.to_owned()).unwrap_or("unknown".to_string());

            let value =
                serde_json::to_value(target).context("Failed to serialize merged object")?;
            let object: DynamicObject = serde_json::from_value(value)?;
            Ok(MergeResult::Create { object })
        }
    }

    fn merge_delete(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &String,
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


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnyApplicationOwnership {
    pub owner: String,
    pub placements: Vec<String>
}


#[cfg(test)]
pub mod tests {

    #[test]
    pub fn test_any_application_merge() {}
}
