pub mod api;
pub mod config;
pub mod context;
pub mod context_builder;
mod http;
pub mod metrics;
pub mod tracing;

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[cfg(test)]
mod tests;
