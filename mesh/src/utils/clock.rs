use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use crate::utils::types::Clock;

#[derive(Debug, Clone)]
pub struct RealClock {}

impl Default for RealClock {
    fn default() -> Self {
        Self::new()
    }
}

impl RealClock {
    pub fn new() -> Self {
        RealClock {}
    }
}

impl Clock for RealClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64
    }
}

#[derive(Debug, Clone)]
pub struct FakeClock {
    time: Arc<AtomicU64>,
}

impl Default for FakeClock {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeClock {
    pub fn new() -> Self {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        FakeClock {
            time: Arc::new(AtomicU64::new(time)),
        }
    }

    pub fn set_time(&self, time: SystemTime) {
        self.time.store(
            time.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            Ordering::SeqCst,
        );
    }

    pub fn set_time_millis(&self, millis: u64) {
        let new_time = SystemTime::UNIX_EPOCH + Duration::from_millis(millis);
        self.set_time(new_time);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(self.time.load(Ordering::SeqCst))
    }

    fn now_millis(&self) -> u64 {
        self.time.load(Ordering::SeqCst)
    }
}
