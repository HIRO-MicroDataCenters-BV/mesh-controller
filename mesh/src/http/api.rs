use crate::{
    api::{status::HealthStatus, MeshApi},
    config::configuration::Config,
};
use anyhow::Result;
use axum::async_trait;
use axum_prometheus::{
    metrics::set_global_recorder,
    metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle},
};
use tracing::warn;

pub struct MeshApiImpl {
    _config: Config,
    recorder_handle: PrometheusHandle,
}

impl MeshApiImpl {
    pub fn new(config: Config) -> Result<MeshApiImpl> {
        let recorder_handle = setup_metrics_recorder()?;
        Ok(MeshApiImpl {
            _config: config,
            recorder_handle,
        })
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
        Ok(self.recorder_handle.render())
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
