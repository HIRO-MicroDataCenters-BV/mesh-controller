use std::time::SystemTime;

pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
    fn now_millis(&self) -> u64;
}
