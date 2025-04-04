use anyhow::{bail, Context as AnyhowContext, Result};
use p2panda_core::PublicKey;
use tokio::runtime::Runtime;
use tokio::signal;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::configuration::Config;

pub struct Context {
    _config: Config,
    _public_key: PublicKey,
    http_handle: JoinHandle<Result<()>>,
    http_runtime: Runtime,
    cancellation_token: CancellationToken,
    mesh_runtime: Runtime,
}

impl Context {
    pub fn new(
        config: Config,
        public_key: PublicKey,
        http_handle: JoinHandle<Result<()>>,
        http_runtime: Runtime,
        cancellation_token: CancellationToken,
        mesh_runtime: Runtime,
    ) -> Self {
        Context {
            _config: config,
            _public_key: public_key,
            http_handle,
            http_runtime,
            cancellation_token,
            mesh_runtime,
        }
    }

    pub fn configure(&self) -> Result<()> {
        self.mesh_runtime.block_on(async {
            self.configure_inner()
                .await
                .context("failed to configure Node")
        })
    }

    async fn configure_inner(&self) -> Result<()> {
        // TODO subscribe membership and message sync
        Ok(())
    }

    pub fn wait_for_termination(&self) -> Result<()> {
        let cloned_token = self.cancellation_token.clone();
        self.http_runtime.block_on(async move {
            tokio::select! {
                _ = cloned_token.cancelled() => bail!("HTTP server was cancelled"),
                _ = signal::ctrl_c() => {},
            };
            Ok(())
        })
    }

    pub fn shutdown(self) -> Result<()> {
        self.http_handle.abort();
        self.http_runtime.shutdown_background();
        self.mesh_runtime.shutdown_background();
        Ok(())
    }
}
