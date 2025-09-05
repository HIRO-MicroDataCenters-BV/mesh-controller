use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, Default)]
pub enum KubeConfiguration {
    #[serde(rename = "incluster")]
    #[default]
    InCluster,
    #[serde(rename = "external")]
    External(KubeConfigurationExternal),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct KubeConfigurationExternal {
    pub kube_context: Option<String>,
}
