use anyhow::Result;
use iroh_metrics::core::{Core, Metric};
use mesh::{built_info, context_builder::ContextBuilder};
use tracing::info;

fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to setup rustls default crypto provider [aws_lc_rs]");

    Core::init(|reg, metrics| {
        metrics.insert(iroh::metrics::ConnectionMetrics::new(reg));
        metrics.insert(iroh::metrics::MagicsockMetrics::new(reg));
        metrics.insert(iroh::metrics::NetReportMetrics::new(reg));
        metrics.insert(iroh_quinn::metrics::ConnectionDriverMetrics::new(reg));
        metrics.insert(iroh_quinn::metrics::QuinnConnectionMetrics::new(reg));
        metrics.insert(iroh_quinn::metrics::RuntimeMetrics::new(reg));
        metrics.insert(iroh_quinn::metrics::ConnectionSetMetrics::new(reg));
    });

    let context_builder = ContextBuilder::from_cli()?;
    let context = context_builder.try_build_and_start()?;

    context.configure()?;

    log_startup_string();

    context.wait_for_termination()?;

    info!("shutting down");

    context.shutdown()
}

fn log_startup_string() {
    print_startup_string(
        env!("CARGO_PKG_DESCRIPTION"),
        env!("CARGO_PKG_VERSION"),
        built_info::GIT_VERSION,
        built_info::TARGET,
        built_info::BUILT_TIME_UTC,
        built_info::RUSTC_VERSION,
    );
    info!("");
}

pub fn print_startup_string(
    pkg_description: &str,
    pkg_version: &str,
    git_version: Option<&str>,
    target: &str,
    built_time: &str,
    rustc_version: &str,
) {
    let git = match git_version {
        None => "".to_string(),
        Some(git) => format!(" (Git information: {git})"),
    };
    info!("Starting {pkg_description}");
    info!(
        "This is version {pkg_version}{git}, built for {target} by {rustc_version} at {built_time}",
    )
}
