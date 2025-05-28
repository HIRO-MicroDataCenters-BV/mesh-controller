#[cfg(test)]
pub mod tests {

    use std::collections::BTreeMap;

    use kube::api::{DynamicObject, ObjectMeta};

    use crate::{
        kube::cache::Version,
        merge::{
            anyapplication::{
                AnyApplication, AnyApplicationApplication, AnyApplicationApplicationHelm,
                AnyApplicationSpec, AnyApplicationStatus, AnyApplicationStatusConditions,
                AnyApplicationStatusPlacements,
            },
            anyapplication_ext::OWNER_VERSION,
        },
    };

    pub fn make_anyapplication(
        owner_version: Version,
        owner_zone: &str,
        zones: i64,
    ) -> DynamicObject {
        let resource = AnyApplication {
            metadata: ObjectMeta {
                name: Some("nginx-app".into()),
                namespace: Some("default".into()),
                labels: Some(BTreeMap::from([(
                    OWNER_VERSION.into(),
                    owner_version.to_string(),
                )])),
                ..Default::default()
            },
            spec: AnyApplicationSpec {
                application: AnyApplicationApplication {
                    helm: Some(AnyApplicationApplicationHelm {
                        chart: "chart".into(),
                        version: "1.0.0".into(),
                        namespace: "namespace".into(),
                        repository: "repo".into(),
                        values: None,
                    }),
                    resource_selector: None,
                },
                placement_strategy: None,
                recover_strategy: None,
                zones: zones,
            },
            status: Some(AnyApplicationStatus {
                conditions: None,
                owner: owner_zone.into(),
                placements: Some(vec![
                    AnyApplicationStatusPlacements {
                        node_affinity: None,
                        zone: owner_zone.into(),
                    },
                    AnyApplicationStatusPlacements {
                        node_affinity: None,
                        zone: "zone2".into(),
                    },
                ]),
                state: "New".into(),
            }),
        };
        let resource_str = serde_json::to_value(&resource).expect("Resource is not serializable");
        let object: DynamicObject =
            serde_json::from_value(resource_str).expect("Cannot parse dynamic object");
        object
    }

    pub fn anycondition(
        owner_version: Version,
        owner_zone: &str,
        cond_type: &str,
    ) -> AnyApplicationStatusConditions {
        AnyApplicationStatusConditions {
            last_transition_time: "time".into(),
            msg: None,
            reason: None,
            status: "status".into(),
            r#type: cond_type.into(),
            zone_id: owner_zone.into(),
            zone_version: owner_version.to_string(),
        }
    }
}
