use std::collections::BTreeMap;

use anyhow::Result;
use kube::api::DynamicObject;

use super::types::CacheProtocol;

#[derive(Debug, Default)]
pub struct KubeApi {}

impl KubeApi {
    pub fn new() -> KubeApi {
        KubeApi {}
    }

    pub async fn publish(&self, _object: DynamicObject) -> Result<()> {
        Ok(())
    }

    pub fn snapshot(&self) -> Result<CacheProtocol> {
        Ok(CacheProtocol::Snapshot {
            snapshot: BTreeMap::new(),
        })
    }
}
