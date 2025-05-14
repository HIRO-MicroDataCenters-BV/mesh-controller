use crate::{config::configuration::Config, context::Context, context_builder::ContextBuilder};
use anyhow::Result;
use p2panda_core::PrivateKey;

pub struct FakeMeshServer {
    context: Context,
}

impl FakeMeshServer {
    pub fn try_start(config: Config, private_key: PrivateKey) -> Result<Self> {
        let builder = ContextBuilder::new(config, private_key);
        let context = builder.try_build_and_start()?;
        context.configure()?;
        Ok(FakeMeshServer { context })
    }

    pub fn discard(self) -> Result<()> {
        self.context.shutdown()
    }
}
