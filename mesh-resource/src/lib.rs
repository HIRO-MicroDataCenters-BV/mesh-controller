pub mod actor;
pub mod meshpeer;
pub mod peers;
pub mod types;

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
