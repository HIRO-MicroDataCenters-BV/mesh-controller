// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;

use p2panda_core::PublicKey;
use tracing::{debug, warn};

/// Manages buffering of gossip messages during sync sessions.
///
/// # Memory Leak Warning
///
/// **IMPORTANT:** The `counters` HashMap grows unbounded. Counter entries are never removed
/// even when they reach zero or when buffers are drained. Each unique (PublicKey, topic_id)
/// pair creates a permanent entry.
///
/// ## Known Issues:
/// - Counter entries persist even after reaching 0
/// - `drain()` removes buffer but not counter
/// - No cleanup mechanism for old entries
///
/// ## Mitigation Strategies:
/// - Remove counter entries when they reach 0
/// - Clean up counters in `drain()` method
/// - Add periodic cleanup for zero-value counters
///
/// See MEMORY_LEAK_ANALYSIS.md for detailed information.
#[derive(Debug, Default)]
pub struct GossipBuffer {
    buffers: HashMap<(PublicKey, [u8; 32]), Vec<Vec<u8>>>,
    // FIXME: This HashMap grows unbounded - counters are never removed even when reaching zero
    // The drain() method removes buffers but not counters, causing a leak
    counters: HashMap<(PublicKey, [u8; 32]), usize>,
}

impl GossipBuffer {
    pub fn lock(&mut self, peer: PublicKey, topic_id: [u8; 32]) {
        let counter = self.counters.entry((peer, topic_id)).or_default();
        *counter += 1;

        self.buffers.entry((peer, topic_id)).or_default();

        debug!(
            "lock gossip buffer with {} on topic {:?}: {}",
            peer, topic_id, counter
        );
    }

    pub fn unlock(&mut self, peer: PublicKey, topic_id: [u8; 32]) -> Option<usize> {
        // Only decrement the counter if it exists and is greater than zero.
        match self.counters.get_mut(&(peer, topic_id)) {
            Some(counter) if *counter > 0 => {
                *counter -= 1;
                debug!(
                    "unlock gossip buffer with {} on topic {:?}: {}",
                    peer, topic_id, counter
                );
                Some(*counter)
            }
            _ => {
                warn!(
                    "attempted to unlock non-existing gossip buffer with {} on topic {:?}",
                    peer, topic_id
                );
                None
            }
        }
    }

    pub fn drain(&mut self, peer: PublicKey, topic_id: [u8; 32]) -> Option<Vec<Vec<u8>>> {
        self.buffers.remove(&(peer, topic_id))
    }

    pub fn buffer(&mut self, peer: PublicKey, topic_id: [u8; 32]) -> Option<&mut Vec<Vec<u8>>> {
        self.buffers.get_mut(&(peer, topic_id))
    }
}

#[cfg(test)]
mod tests {
    use p2panda_core::PrivateKey;

    use super::GossipBuffer;

    #[tokio::test]
    async fn lock_and_unlock_buffer() {
        let private_key = PrivateKey::new();
        let peer = private_key.public_key();
        let topic_id = [9; 32];

        let mut buffer = GossipBuffer::default();

        // Lock the buffer.
        buffer.lock(peer, topic_id);

        // Counter should exist and have a value of 1.
        let counter = buffer.counters.get(&(peer, topic_id));
        assert!(counter.is_some());
        assert_eq!(*counter.unwrap(), 1);

        // Unlock the buffer.
        buffer.unlock(peer, topic_id);

        // Counter should exist and have a value of 0.
        let counter = buffer.counters.get(&(peer, topic_id));
        assert!(counter.is_some());
        assert_eq!(*counter.unwrap(), 0);

        // Unlock the buffer again.
        buffer.unlock(peer, topic_id);

        // Counter should exist and have a value of 0.
        // No subtract overflow panic should occur.
        let counter = buffer.counters.get(&(peer, topic_id));
        assert!(counter.is_some());
        assert_eq!(*counter.unwrap(), 0);

        let unknown_topic_id = [8; 32];

        // Unlock the buffer for an unknown topic.
        buffer.unlock(peer, unknown_topic_id);

        // Counter should not exist.
        let counter = buffer.counters.get(&(peer, unknown_topic_id));
        assert!(counter.is_none());
    }
}
