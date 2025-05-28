use super::anyapplication::AnyApplication;
use crate::kube::cache::Version;
use anyhow::{Result, anyhow};

const OWNER_VERSION: &str = "dcp.hiro.io/owner-version";

pub trait AnyApplicationExt {
    fn get_owner_version(&self) -> Result<Version>;
    fn set_owner_version(&mut self, version: Version);
    fn get_owner_zone(&self) -> String;
}

impl AnyApplicationExt for AnyApplication {
    fn get_owner_version(&self) -> Result<Version> {
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

    fn get_owner_zone(&self) -> String {
        self.status
            .as_ref()
            .map(|s| s.owner.to_owned())
            .unwrap_or("unknown".to_string())
    }
}

pub trait AnyApplicationStatusConditionsExt {
    fn get_owner_version(&self) -> Result<Version>;
    fn set_owner_version(&mut self, version: Version);
    fn get_owner_zone(&self) -> String;
}
