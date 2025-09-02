use anyhow::Result;
use meshresource::{built_info, meshpeer::MeshPeer};
use stackable_operator::CustomResourceExt;

pub fn main() -> Result<()> {
    MeshPeer::print_yaml_schema(built_info::PKG_VERSION)?;
    Ok(())
}
