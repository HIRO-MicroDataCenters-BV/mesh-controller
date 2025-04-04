use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, PartialEq)]
pub struct HealthStatus {
    pub status: ServiceStatus,
    pub msg: Option<String>,
}

impl From<anyhow::Error> for HealthStatus {
    fn from(error: anyhow::Error) -> Self {
        HealthStatus {
            status: ServiceStatus::Error,
            msg: Some(format!("{:?}", error)),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, PartialEq)]
pub enum ServiceStatus {
    Running,
    #[default]
    Unknown,
    Error,
}
