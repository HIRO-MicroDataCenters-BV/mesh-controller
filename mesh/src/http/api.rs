use crate::api::{MeshApi, status::HealthStatus};
use anyhow::Result;
use async_trait::async_trait;
use axum_prometheus::{
    metrics::set_global_recorder,
    metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle},
};
use iroh_metrics::core::Core;
use tracing::warn;

pub struct MeshApiImpl {
    recorder_handle: PrometheusHandle,
}

impl MeshApiImpl {
    pub fn new() -> Result<MeshApiImpl> {
        let recorder_handle = setup_metrics_recorder()?;
        Ok(MeshApiImpl { recorder_handle })
    }
}

#[async_trait]
impl MeshApi for MeshApiImpl {
    async fn health(&self) -> Result<HealthStatus> {
        Ok(HealthStatus {
            status: crate::api::status::ServiceStatus::Running,
            msg: None,
        })
    }

    async fn metrics(&self) -> Result<String> {
        let mut metrics_rendered = self.recorder_handle.render();
        let core = Core::get().ok_or(iroh_metrics::Error::NoMetrics)?;
        let content = core.encode();
        metrics_rendered += &content;
        Ok(metrics_rendered)
    }
}

fn setup_metrics_recorder() -> Result<PrometheusHandle> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();

    let maybe_success = set_global_recorder(recorder);
    if let Err(e) = &maybe_success {
        let msg = format!("{}", e);
        if msg.contains(
            "attempted to set a recorder after the metrics system was already initialized",
        ) {
            warn!("global recorder is possibly reused.")
        } else {
            maybe_success?;
        }
    }
    Ok(handle)
}
