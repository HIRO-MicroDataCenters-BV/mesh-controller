use crate::kube::types::CacheError;

use super::types::KubeClient;

pub struct KubeClientImpl {}

impl KubeClientImpl {}

impl KubeClient for KubeClientImpl {
    fn try_init() -> Result<Self, CacheError> {
        Ok(KubeClientImpl {})
    }
}
