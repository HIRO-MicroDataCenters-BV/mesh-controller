use anyapplication::anyapplication::{
    AnyApplication, AnyApplicationStatusOwnership, AnyApplicationStatusOwnershipPlacements,
    AnyApplicationStatusZones,
};
use anyapplication::anyapplication_ext::Epoch;
use anyhow::{Context, Result, anyhow};
use k8s_openapi::api::core::v1::ObjectReference;
use kube::ResourceExt;
use kube::api::{DynamicObject, GroupVersionKind};
use serde_json::{Value, json};

use super::subscriptions::Version;
use super::types::NamespacedName;

const OWNER_VERSION: &str = "dcp.hiro.io/owner-version";
const OWNER_ZONE: &str = "dcp.hiro.io/owner-zone";
const OWNER_EPOCH: &str = "dcp.hiro.io/owner-epoch";

pub trait DynamicObjectExt {
    fn get_gvk(&self) -> Result<GroupVersionKind>;
    fn get_namespaced_name(&self) -> NamespacedName;
    fn get_first_item_or_fail(&self) -> Result<Option<DynamicObject>>;
    fn get_owner_version_or_fail(&self) -> Result<Version>;
    fn get_owner_version(&self) -> Option<Version>;
    fn set_owner_version(&mut self, version: Version);
    fn get_owner_zone(&self) -> Result<String>;
    fn set_owner_zone(&mut self, zone: String);
    fn get_owner_epoch(&self) -> Result<Epoch>;
    fn set_owner_epoch(&mut self, epoch: Epoch);
    fn normalize(&mut self, default_zone: &str);
    fn get_status(&self) -> Option<Value>;
    fn unset_resource_version(&mut self);
    fn get_resource_version(&self) -> Version;
    fn has_resource_version(&self) -> bool;
    fn set_resource_version(&mut self, version: Version);
    fn dump_status(&self, loc: &str);
    fn get_object_reference(&self) -> Result<ObjectReference>;
}

impl DynamicObjectExt for DynamicObject {
    fn get_namespaced_name(&self) -> NamespacedName {
        let ns = self.namespace().unwrap_or("default".into());
        let name = self.name_any();
        NamespacedName::new(ns, name)
    }

    fn get_gvk(&self) -> Result<GroupVersionKind> {
        let types = self
            .types
            .as_ref()
            .ok_or_else(|| anyhow!("Missing TypeMeta in DynamicObject"))?;
        let api_version = &types.api_version;
        let kind = &types.kind;

        // Split apiVersion into group and version
        let (group, version) = if let Some((g, v)) = api_version.split_once('/') {
            (g.to_string(), v.to_string())
        } else {
            ("".to_string(), api_version.clone())
        };

        Ok(GroupVersionKind {
            group,
            version,
            kind: kind.clone(),
        })
    }

    fn get_first_item_or_fail(&self) -> Result<Option<DynamicObject>> {
        let gvk = self
            .get_gvk()
            .context("cannot get GroupVersionKind of result")?;
        if gvk.kind.ends_with("List") {
            let object = self
                .data
                .as_object()
                .ok_or(anyhow!("direct_get, 'items' object is expected"))?;
            let items = object
                .get("items")
                .and_then(|items| items.as_array())
                .ok_or(anyhow!("direct_get: items object is expected"))?;
            if items.is_empty() {
                return Ok(None);
            } else if items.len() > 1 {
                return Err(anyhow!("more than one item returned"));
            }

            let item = items
                .first()
                .cloned()
                .ok_or(anyhow!("single item is expected"))?;

            let object: DynamicObject = serde_json::from_str(&serde_json::to_string(&item)?)?;
            Ok(Some(object))
        } else {
            Ok(Some(self.clone()))
        }
    }

    fn get_owner_version_or_fail(&self) -> Result<Version> {
        self.metadata
            .labels
            .as_ref()
            .ok_or(anyhow!("{} label not set", OWNER_VERSION))?
            .get(OWNER_VERSION)
            .map(|v| {
                v.parse::<Version>()
                    .map_err(|e| anyhow!("unable to parse version from label. {e}"))
            })
            .unwrap_or(Err(anyhow!("{} label not set", OWNER_VERSION)))
    }

    fn set_owner_version(&mut self, version: Version) {
        let labels = self.metadata.labels.get_or_insert_default();
        labels.insert(OWNER_VERSION.into(), version.to_string());
    }

    fn get_owner_zone(&self) -> Result<String> {
        self.metadata
            .labels
            .as_ref()
            .ok_or(anyhow!("{} label not set", OWNER_ZONE))?
            .get(OWNER_ZONE)
            .cloned()
            .ok_or(anyhow!("{} label not set", OWNER_ZONE))
            .inspect_err(|_| {
                // TODO remove temporary logging
                tracing::info!("label not set {:?}", &self)
            })
    }

    fn get_owner_version(&self) -> Option<Version> {
        let labels = self.metadata.labels.as_ref()?;
        let version_str = labels.get(OWNER_VERSION)?;
        version_str.parse::<Version>().map(Some).unwrap_or_default()
    }

    fn get_owner_epoch(&self) -> Result<Epoch> {
        let labels = self
            .metadata
            .labels
            .as_ref()
            .ok_or(anyhow!("{} label not set", OWNER_EPOCH))?;
        let epoch_str = labels
            .get(OWNER_EPOCH)
            .ok_or(anyhow!("{} label not set", OWNER_EPOCH))?;
        epoch_str.parse::<Epoch>().map_err(|e| e.into())
    }

    fn set_owner_epoch(&mut self, epoch: Epoch) {
        let labels = self.metadata.labels.get_or_insert_default();
        labels.insert(OWNER_EPOCH.into(), epoch.to_string());
    }

    fn set_owner_zone(&mut self, zone: String) {
        let labels = self.metadata.labels.get_or_insert_default();
        labels.insert(OWNER_ZONE.into(), zone.to_string());
    }

    fn normalize(&mut self, default_zone: &str) {
        self.metadata.managed_fields = None;
        self.metadata.uid = None;
        if self.get_owner_zone().is_err() {
            self.set_owner_zone(default_zone.into());
        }
    }

    fn get_status(&self) -> Option<Value> {
        self.data
            .get("status")
            .map(|status| json!({ "status": status }))
    }

    fn unset_resource_version(&mut self) {
        self.metadata.resource_version = None;
    }

    fn get_resource_version(&self) -> Version {
        let maybe_resource_version = &self.metadata.resource_version.as_ref();

        // TODO remove temporary logging
        if maybe_resource_version.is_none() {
            tracing::info!("no resource version {:?}", &self)
        }

        let resource_version = maybe_resource_version.expect("resource version is not set");
        resource_version
            .parse()
            .expect("resource version must be numberic")
    }

    fn has_resource_version(&self) -> bool {
        self.metadata.resource_version.as_ref().is_some()
    }

    fn set_resource_version(&mut self, version: Version) {
        self.metadata.resource_version = Some(version.to_string());
    }

    fn dump_status(&self, context: &str) {
        let app: AnyApplication = self.clone().try_parse().unwrap();
        let Some(status) = app.status else { return };
        dump_ownership(context, &status.ownership);
        let Some(zones) = status.zones else { return };
        dump_zones(context, &zones);
    }

    fn get_object_reference(&self) -> Result<ObjectReference> {
        let gvk = self.get_gvk()?;
        Ok(ObjectReference {
            api_version: Some(gvk.api_version()),
            kind: Some(gvk.kind),
            name: self.metadata.name.to_owned(),
            namespace: self.namespace(),
            uid: self.uid(),
            ..Default::default()
        })
    }
}

pub fn dump_ownership(context: &str, ownership: &AnyApplicationStatusOwnership) {
    let mut out = format!("- ownership update - ({})\n", context);
    out += format!(" epoch: {}\n", ownership.epoch).as_str();
    out += format!(" owner: {}\n", ownership.owner).as_str();
    out += format!(" state: {}\n", ownership.state).as_str();
    out += format!(
        " place: {}\n",
        render_placements(ownership.placements.as_ref().unwrap_or(&vec![]))
    )
    .as_str();
    println!("{}", out);
}

pub fn dump_zones(context: &str, zones: &[AnyApplicationStatusZones]) {
    let mut out = format!("- status update - ({})\n", context);

    for zone in zones.iter() {
        out += format!(" zone: {}\n", zone.zone_id).as_str();
        out += format!("  - version: {}\n", zone.version).as_str();
        let Some(conditions) = &zone.conditions else {
            continue;
        };
        out += "  - conditions:\n";
        for cond in conditions {
            out += format!("   -- {}, {}\n", cond.r#type, cond.status).as_str();
        }
    }
    out += "\n";
    println!("{}", out);
}

pub fn render_placements(placements: &[AnyApplicationStatusOwnershipPlacements]) -> String {
    let mut out = String::new();

    let mut sep = false;
    for placement in placements.iter() {
        if sep {
            out += ", ";
        }
        out += &placement.zone;
        sep = true;
    }
    out
}
