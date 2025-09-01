pub mod client;
pub mod config;
pub mod kube;

pub(crate) type JoinErrToStr =
    Box<dyn Fn(tokio::task::JoinError) -> String + Send + Sync + 'static>;
