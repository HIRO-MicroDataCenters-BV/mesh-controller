use crate::{
    api::status::HealthStatus,
    api::MeshApi,
    api::{HTTP_HEALTH_ROUTE, HTTP_METRICS_ROUTE},
};
use anyhow::{Context, Result};
use async_trait::async_trait;

pub struct MeshApiClient {
    endpoint: String,
}

impl MeshApiClient {
    pub fn new(endpoint: String) -> MeshApiClient {
        MeshApiClient { endpoint }
    }
}

#[async_trait]
impl MeshApi for MeshApiClient {
    async fn health(&self) -> Result<HealthStatus> {
        let url = format!("{}{}", self.endpoint, HTTP_HEALTH_ROUTE);
        let response = reqwest::get(url)
            .await
            .context("health request")?
            .json::<HealthStatus>()
            .await
            .context("health response deserialization")?;
        Ok(response)
    }

    async fn metrics(&self) -> Result<String> {
        let url = format!("{}{}", self.endpoint, HTTP_METRICS_ROUTE);
        let response = reqwest::get(url)
            .await
            .context("metrics request")?
            .text()
            .await
            .context("metrics response deserialization")?;
        Ok(response)
    }
}
