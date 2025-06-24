#[cfg(test)]
pub mod tests {

    use std::collections::BTreeMap;

    use anyapplication::{
        anyapplication::{
            AnyApplication, AnyApplicationApplication, AnyApplicationApplicationHelm,
            AnyApplicationSpec, AnyApplicationStatus, AnyApplicationStatusPlacements,
            AnyApplicationStatusZones, AnyApplicationStatusZonesConditions,
        },
        anyapplication_ext::OWNER_VERSION,
    };
    use kube::api::{DynamicObject, ObjectMeta};

    use crate::kube::subscriptions::Version;

    pub fn anyapp(owner_version: Version, owner_zone: &str, zones: i64) -> DynamicObject {
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
                zones: None,
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

    pub fn make_anyapplication_with_conditions(
        owner_version: Version,
        resource_version: Version,
        owner_zone: &str,
        zones: i64,
        zones_statuses: &[AnyApplicationStatusZones],
    ) -> DynamicObject {
        let mut object = AnyApplication {
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
                zones: Some(zones_statuses.into()),
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

        object.metadata.resource_version = Some(resource_version.to_string());

        let resource_str = serde_json::to_value(&object).expect("Resource is not serializable");
        let object: DynamicObject =
            serde_json::from_value(resource_str).expect("Cannot parse dynamic object");
        object
    }

    pub fn anyzone(
        zone_id: &str,
        version: i64,
        conditions: &[AnyApplicationStatusZonesConditions],
    ) -> AnyApplicationStatusZones {
        AnyApplicationStatusZones {
            zone_id: zone_id.into(),
            version,
            conditions: Some(conditions.into()),
        }
    }

    pub fn anycond(owner_zone: &str, cond_type: &str) -> AnyApplicationStatusZonesConditions {
        AnyApplicationStatusZonesConditions {
            last_transition_time: "time".into(),
            msg: None,
            reason: None,
            status: "status".into(),
            r#type: cond_type.into(),
            zone_id: owner_zone.into(),
        }
    }

    pub fn anycond_status(
        owner_zone: &str,
        cond_type: &str,
        status: &str,
    ) -> AnyApplicationStatusZonesConditions {
        AnyApplicationStatusZonesConditions {
            last_transition_time: "time".into(),
            msg: None,
            reason: None,
            status: status.into(),
            r#type: cond_type.into(),
            zone_id: owner_zone.into(),
        }
    }

    pub fn anyplacements(zone1: &str, zone2: Option<&str>) -> Vec<AnyApplicationStatusPlacements> {
        let mut placements = vec![AnyApplicationStatusPlacements {
            zone: zone1.into(),
            node_affinity: None,
        }];
        if let Some(zone2) = zone2 {
            placements.push(AnyApplicationStatusPlacements {
                zone: zone2.into(),
                node_affinity: None,
            });
        }
        placements
    }

    pub fn anystatus(
        owner_zone: &str,
        placements: Vec<AnyApplicationStatusPlacements>,
        zones: Option<Vec<AnyApplicationStatusZones>>,
    ) -> AnyApplicationStatus {
        AnyApplicationStatus {
            zones: zones,
            owner: owner_zone.into(),
            placements: Some(placements),
            state: "New".into(),
        }
    }

    pub fn anyspec(zones: i64) -> AnyApplicationSpec {
        AnyApplicationSpec {
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
        }
    }
}
