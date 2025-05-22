use anyhow::Result;
use kube::api::DynamicObject;

#[derive(Debug, Clone, Default)]
pub struct KubeApi {
    
}

impl KubeApi {
    pub fn new() -> KubeApi {
        KubeApi {}
    }

    pub async fn apply_update(&self, _source: &String, _object: DynamicObject) -> Result<()> {
        Ok(())
    }

    pub async fn apply_snapshot(&self, _source: &String, _object: DynamicObject) -> Result<()> {
        Ok(())
    }

}
