use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Unknown error")]
    Unknown,
}

// pub trait KubeCache {
//     type R: Resource<DynamicType = ApiResource> + Clone + Debug + Send + DeserializeOwned + 'static;

//     fn get(gvk : &GroupVersionKind) -> Result<Option<Self::R>, KubeError>;
// }
