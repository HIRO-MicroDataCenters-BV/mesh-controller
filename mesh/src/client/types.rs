use kube::{
    Resource,
    api::{ListParams, ObjectList},
    runtime::reflector::ObjectRef,
};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

use crate::kube::types::CacheError;

#[async_trait::async_trait]
pub trait KubeClient {
    fn try_init() -> Result<Self, CacheError>
    where
        Self: Sized;
}
