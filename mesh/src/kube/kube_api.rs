use anyhow::Result;
use kube::api::DynamicObject;

pub struct KubeApi {}

impl KubeApi {
    pub fn new() -> KubeApi {
        KubeApi {}
    }

    pub async fn publish(&self, _object: DynamicObject) -> Result<()> {
        Ok(())
    }
}
