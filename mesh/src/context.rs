use anyhow::{Context as AnyhowContext, Result, bail};
use kube::api::GroupVersionKind;
use meshkube::kube::subscriptions::Subscriptions;
use p2panda_core::PublicKey;
use tokio::runtime::Runtime;
use tokio::signal;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::configuration::Config;
use crate::mesh::topic::MeshTopic;
use crate::node::mesh::MeshNode;

pub struct Context {
    config: Config,
    _public_key: PublicKey,
    mesh_node: MeshNode,
    subscriptions: Subscriptions,
    http_handle: JoinHandle<Result<()>>,
    http_runtime: Runtime,
    cancellation: CancellationToken,
    mesh_runtime: Runtime,
}

impl Context {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        mesh_node: MeshNode,
        subscriptions: Subscriptions,
        public_key: PublicKey,
        http_handle: JoinHandle<Result<()>>,
        http_runtime: Runtime,
        cancellation: CancellationToken,
        mesh_runtime: Runtime,
    ) -> Self {
        Self {
            config,
            _public_key: public_key,
            mesh_node,
            subscriptions,
            http_handle,
            http_runtime,
            cancellation,
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
        self.subscribe(
            self.config.mesh.resource.get_gvk(),
            self.config.mesh.resource.namespace.to_owned(),
            MeshTopic::default(),
        )
        .await?;

        Ok(())
    }

    async fn subscribe(
        &self,
        gvk: GroupVersionKind,
        namespace: Option<String>,
        topic: MeshTopic,
    ) -> Result<()> {
        let (subscriber_rx, _) = self.subscriptions.subscribe(&gvk, &namespace).await?;
        self.mesh_node
            .subscribe_mesh(topic, Box::pin(subscriber_rx.into_stream()))
            .await
    }

    pub fn wait_for_termination(&self) -> Result<()> {
        let cloned_token = self.cancellation.clone();
        self.http_runtime.block_on(async move {
            tokio::select! {
                _ = cloned_token.cancelled() => bail!("HTTP server was cancelled"),
                _ = signal::ctrl_c() => {},
            };
            Ok(())
        })
    }

    pub fn shutdown(self) -> Result<()> {
        self.mesh_runtime.block_on(async move {
            self.mesh_node
                .shutdown()
                .await
                .context("Failure during node shutdown")
        })?;
        self.http_handle.abort();
        self.http_runtime.shutdown_background();
        self.mesh_runtime.shutdown_background();
        Ok(())
    }
}
