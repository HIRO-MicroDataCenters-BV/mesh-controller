use crate::mesh::operations::Extensions;
use anyhow::Result;
use p2panda_core::Operation;
use p2panda_stream::operation::IngestError;

pub trait OperationExt {
    fn get_id(&self) -> Result<String>;
}

impl OperationExt for Operation<Extensions> {
    fn get_id(&self) -> Result<String> {
        let Some(extensions) = self.header.extensions.as_ref() else {
            return Err(IngestError::MissingHeaderExtension("extension".into()).into());
        };
        let seq_num = self.header.seq_num;
        let log_id = &extensions.log_id;
        Ok(format!("{}#{}", log_id.0, seq_num))
    }
}