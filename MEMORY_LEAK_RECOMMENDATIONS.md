# Memory Leak Mitigation Recommendations

This document provides specific, actionable recommendations to address the memory leak issues identified in the patch directory crates.

## Quick Reference

| Issue | Severity | Effort | Priority |
|-------|----------|--------|----------|
| AddressBook unbounded growth | HIGH | 3-5 days | 1 |
| TopicStreams subscription leak | MEDIUM-HIGH | 3-4 days | 2 |
| GossipBuffer counter leak | MEDIUM | < 1 day | 3 |
| MemoryStore monitoring | MEDIUM | 2-3 days | 4 |
| Detached task tracking | MEDIUM | 3-4 days | 5 |

## Detailed Recommendations

### 1. AddressBook: Implement Peer Cleanup (HIGH PRIORITY)

**File:** `patch/p2panda/p2panda-net/src/engine/address_book.rs`

**Option A: Add TTL-based cleanup (Recommended)**

```rust
use std::time::{Duration, Instant};

struct PeerEntry {
    addresses: HashSet<NodeAddress>,
    last_seen: Instant,
}

struct AddressBookInner {
    known_peer_topic_ids: HashMap<PublicKey, HashSet<[u8; 32]>>,
    known_peer_entries: HashMap<PublicKey, PeerEntry>,
    peer_ttl: Duration,
}

impl AddressBook {
    pub fn new(network_id: NetworkId, peer_ttl: Duration) -> Self {
        // ... initialize with TTL
    }

    /// Update peer last_seen timestamp
    pub async fn touch_peer(&mut self, public_key: PublicKey) {
        let mut inner = self.inner.write().await;
        if let Some(entry) = inner.known_peer_entries.get_mut(&public_key) {
            entry.last_seen = Instant::now();
        }
    }

    /// Remove peers that haven't been seen within TTL
    pub async fn cleanup_stale_peers(&mut self) -> Vec<PublicKey> {
        let mut inner = self.inner.write().await;
        let now = Instant::now();
        let mut removed = Vec::new();

        inner.known_peer_entries.retain(|public_key, entry| {
            if now.duration_since(entry.last_seen) > inner.peer_ttl {
                removed.push(*public_key);
                false
            } else {
                true
            }
        });

        // Also clean up topic IDs for removed peers
        for public_key in &removed {
            inner.known_peer_topic_ids.remove(public_key);
        }

        removed
    }

    /// Spawn a periodic cleanup task
    pub fn spawn_cleanup_task(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let mut address_book = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                interval.tick().await;
                let removed = address_book.cleanup_stale_peers().await;
                if !removed.is_empty() {
                    tracing::debug!("Cleaned up {} stale peers", removed.len());
                }
            }
        })
    }
}
```

**Option B: Add LRU cache with capacity limit**

```rust
use lru::LruCache;
use std::num::NonZeroUsize;

struct AddressBookInner {
    known_peer_topic_ids: LruCache<PublicKey, HashSet<[u8; 32]>>,
    known_peer_addresses: LruCache<PublicKey, HashSet<NodeAddress>>,
}

impl AddressBook {
    pub fn new(network_id: NetworkId, max_peers: usize) -> Self {
        let capacity = NonZeroUsize::new(max_peers).unwrap();
        Self {
            network_id,
            inner: Arc::new(RwLock::new(AddressBookInner {
                known_peer_topic_ids: LruCache::new(capacity),
                known_peer_addresses: LruCache::new(capacity),
            })),
        }
    }
}
```

**Option C: Manual removal API (Minimal change)**

```rust
impl AddressBook {
    /// Explicitly remove a peer from the address book
    pub async fn remove_peer(&mut self, public_key: PublicKey) -> bool {
        let mut inner = self.inner.write().await;
        let had_topics = inner.known_peer_topic_ids.remove(&public_key).is_some();
        let had_addresses = inner.known_peer_addresses.remove(&public_key).is_some();
        had_topics || had_addresses
    }

    /// Remove multiple peers in a single lock acquisition
    pub async fn remove_peers(&mut self, public_keys: &[PublicKey]) -> usize {
        let mut inner = self.inner.write().await;
        let mut removed = 0;
        for public_key in public_keys {
            if inner.known_peer_topic_ids.remove(public_key).is_some() {
                removed += 1;
            }
            inner.known_peer_addresses.remove(public_key);
        }
        removed
    }
}
```

**Recommended Approach:** Start with Option C (manual API) for immediate mitigation, then add Option A (TTL-based) for automatic cleanup in a follow-up.

### 2. TopicStreams: Add Unsubscribe Mechanism (MEDIUM-HIGH PRIORITY)

**File:** `patch/p2panda/p2panda-net/src/engine/topic_streams.rs`

```rust
impl<T> TopicStreams<T>
where
    T: TopicQuery + TopicId + 'static,
{
    /// Unsubscribe from a topic stream
    ///
    /// Removes the stream from all internal tracking structures and attempts
    /// to leave the gossip overlay if no other streams need it.
    pub async fn unsubscribe(&mut self, stream_id: TopicStreamId) -> Result<()> {
        // Remove from subscribed
        let Some((topic, _)) = self.subscribed.remove(&stream_id) else {
            warn!("Attempted to unsubscribe unknown stream {}", stream_id);
            return Ok(());
        };

        let topic_id = topic.id();

        // Remove from topic_id_to_stream
        if let Some(stream_ids) = self.topic_id_to_stream.get_mut(&topic_id) {
            stream_ids.retain(|&id| id != stream_id);
            if stream_ids.is_empty() {
                self.topic_id_to_stream.remove(&topic_id);
                
                // If no more streams for this topic_id, leave gossip
                self.leave_gossip(topic_id).await?;
            }
        }

        // Remove from topic_to_stream
        if let Some(stream_ids) = self.topic_to_stream.get_mut(&topic) {
            stream_ids.retain(|&id| id != stream_id);
            if stream_ids.is_empty() {
                self.topic_to_stream.remove(&topic);
            }
        }

        // Clean up pending gossip
        self.gossip_pending.remove(&topic_id);

        debug!("Unsubscribed stream {} for topic {:?}", stream_id, topic_id);
        Ok(())
    }

    /// Leave a gossip overlay
    async fn leave_gossip(&mut self, topic_id: [u8; 32]) -> Result<()> {
        if !self.has_joined_gossip(topic_id).await {
            return Ok(());
        }

        self.gossip_actor_tx
            .send(ToGossipActor::Leave { topic_id })
            .await?;

        let mut gossip_joined = self.gossip_joined.write().await;
        gossip_joined.remove(&topic_id);

        Ok(())
    }

    /// Automatically detect and clean up closed streams
    ///
    /// This should be called periodically to remove streams whose
    /// channels have been closed.
    pub async fn cleanup_closed_streams(&mut self) -> Result<usize> {
        let mut to_remove = Vec::new();

        for (stream_id, (_, from_network_tx)) in &self.subscribed {
            // Check if the channel is closed by attempting to send
            if from_network_tx.is_closed() {
                to_remove.push(*stream_id);
            }
        }

        let count = to_remove.len();
        for stream_id in to_remove {
            self.unsubscribe(stream_id).await?;
        }

        if count > 0 {
            debug!("Cleaned up {} closed streams", count);
        }

        Ok(count)
    }

    /// Return a stream ID for tracking (useful for explicit unsubscribe)
    pub async fn subscribe_with_id(
        &mut self,
        topic: T,
        from_network_tx: mpsc::Sender<FromNetwork>,
        to_network_rx: mpsc::Receiver<ToNetwork>,
        gossip_ready_tx: oneshot::Sender<()>,
    ) -> Result<TopicStreamId> {
        let stream_id = self.next_stream_id;
        self.subscribe(topic, from_network_tx, to_network_rx, gossip_ready_tx).await?;
        Ok(stream_id)
    }
}
```

**Usage Example:**

```rust
// Application code
let stream_id = topic_streams.subscribe_with_id(topic, tx, rx, ready_tx).await?;

// Later, when done with the topic
topic_streams.unsubscribe(stream_id).await?;

// Or periodically cleanup
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        if let Ok(cleaned) = topic_streams.cleanup_closed_streams().await {
            if cleaned > 0 {
                info!("Cleaned up {} closed streams", cleaned);
            }
        }
    }
});
```

### 3. GossipBuffer: Fix Counter Cleanup (MEDIUM PRIORITY)

**File:** `patch/p2panda/p2panda-net/src/engine/gossip_buffer.rs`

**Simple fix - modify existing methods:**

```rust
impl GossipBuffer {
    pub fn unlock(&mut self, peer: PublicKey, topic_id: [u8; 32]) -> Option<usize> {
        match self.counters.get_mut(&(peer, topic_id)) {
            Some(counter) if *counter > 0 => {
                *counter -= 1;
                let count = *counter;
                
                // Remove counter if it reaches zero and buffer is empty
                if count == 0 && !self.buffers.contains_key(&(peer, topic_id)) {
                    self.counters.remove(&(peer, topic_id));
                    debug!("Removed zero counter for peer {} topic {:?}", peer, topic_id);
                }
                
                debug!("unlock gossip buffer with {} on topic {:?}: {}", peer, topic_id, count);
                Some(count)
            }
            _ => {
                warn!("attempted to unlock non-existing gossip buffer with {} on topic {:?}", peer, topic_id);
                None
            }
        }
    }

    pub fn drain(&mut self, peer: PublicKey, topic_id: [u8; 32]) -> Option<Vec<Vec<u8>>> {
        let buffer = self.buffers.remove(&(peer, topic_id));
        
        // Also remove counter if it's zero
        if let Some(counter) = self.counters.get(&(peer, topic_id)) {
            if *counter == 0 {
                self.counters.remove(&(peer, topic_id));
                debug!("Removed zero counter during drain for peer {} topic {:?}", peer, topic_id);
            }
        }
        
        buffer
    }

    /// Explicitly clean up entries for a specific peer-topic combination
    pub fn cleanup(&mut self, peer: PublicKey, topic_id: [u8; 32]) -> bool {
        let had_buffer = self.buffers.remove(&(peer, topic_id)).is_some();
        let had_counter = self.counters.remove(&(peer, topic_id)).is_some();
        
        if had_buffer || had_counter {
            debug!("Cleaned up gossip buffer state for peer {} topic {:?}", peer, topic_id);
        }
        
        had_buffer || had_counter
    }

    /// Clean up all zero counters and empty buffers
    pub fn cleanup_all_empty(&mut self) -> usize {
        let before = self.counters.len() + self.buffers.len();
        
        self.counters.retain(|_, count| *count > 0);
        self.buffers.retain(|_, buffer| !buffer.is_empty());
        
        let after = self.counters.len() + self.buffers.len();
        let cleaned = before - after;
        
        if cleaned > 0 {
            debug!("Cleaned up {} empty gossip buffer entries", cleaned);
        }
        
        cleaned
    }
}
```

### 4. MemoryStore: Add Monitoring (MEDIUM PRIORITY)

**File:** `patch/p2panda/p2panda-store/src/memory.rs`

```rust
impl<L, E> MemoryStore<L, E> {
    /// Get current memory usage statistics
    pub fn stats(&self) -> MemoryStoreStats {
        let store = self.read_store();
        MemoryStoreStats {
            operation_count: store.operations.len(),
            log_count: store.logs.len(),
            total_entries: store.operations.len() + store.logs.len(),
        }
    }

    /// Check if store size exceeds threshold
    pub fn is_over_threshold(&self, max_operations: usize) -> bool {
        let store = self.read_store();
        store.operations.len() > max_operations
    }

    /// Estimate memory usage in bytes (rough approximation)
    pub fn estimated_memory_bytes(&self) -> usize {
        let store = self.read_store();
        let op_size = std::mem::size_of::<StoredOperation<L, E>>();
        let log_size = std::mem::size_of::<LogMeta>();
        
        store.operations.len() * op_size + 
        store.logs.values().map(|set| set.len() * log_size).sum::<usize>()
    }
}

#[derive(Debug, Clone)]
pub struct MemoryStoreStats {
    pub operation_count: usize,
    pub log_count: usize,
    pub total_entries: usize,
}

impl<L, E> MemoryStore<L, E>
where
    L: LogId + Send + Sync,
    E: Extensions + Send + Sync,
{
    /// Spawn a monitoring task that logs warnings when size exceeds threshold
    pub fn spawn_monitor(
        &self,
        interval: Duration,
        max_operations: usize,
    ) -> tokio::task::JoinHandle<()> {
        let store = self.clone();
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            let mut warned = false;
            
            loop {
                interval_timer.tick().await;
                
                let stats = store.stats();
                
                if stats.operation_count > max_operations {
                    if !warned {
                        warn!(
                            "MemoryStore exceeds threshold: {} operations (max: {})",
                            stats.operation_count, max_operations
                        );
                        warned = true;
                    }
                } else {
                    warned = false;
                }
                
                debug!(
                    "MemoryStore stats: {} operations, {} logs, ~{} bytes",
                    stats.operation_count,
                    stats.log_count,
                    store.estimated_memory_bytes()
                );
            }
        })
    }
}
```

## Implementation Plan

### Phase 1: Quick Wins (Week 1)
1. **GossipBuffer counter cleanup** - Implement the fix (< 1 day)
2. **AddressBook manual removal API** - Add `remove_peer()` method (1 day)
3. **Documentation updates** - Add usage examples (1 day)

### Phase 2: Core Features (Weeks 2-3)
4. **TopicStreams unsubscribe** - Full implementation (3-4 days)
5. **AddressBook TTL cleanup** - Implement automatic cleanup (3-5 days)
6. **MemoryStore monitoring** - Add stats and monitoring (2-3 days)

### Phase 3: Testing & Refinement (Week 4)
7. **Memory leak tests** - Comprehensive test suite (5 days)
8. **Load testing** - Verify under realistic conditions (3 days)
9. **Documentation** - Complete API docs and examples (2 days)

## Testing Strategy

### Unit Tests

```rust
#[tokio::test]
async fn test_address_book_cleanup() {
    let mut book = AddressBook::new([0; 32], Duration::from_secs(60));
    
    // Add peers
    let peer1 = /* ... */;
    book.add_peer(peer1).await;
    
    // Advance time
    tokio::time::sleep(Duration::from_secs(61)).await;
    
    // Cleanup should remove stale peer
    let removed = book.cleanup_stale_peers().await;
    assert_eq!(removed.len(), 1);
}

#[tokio::test]
async fn test_topic_streams_unsubscribe() {
    let mut streams = TopicStreams::new(/* ... */);
    
    // Subscribe
    let stream_id = streams.subscribe_with_id(/* ... */).await?;
    
    // Unsubscribe
    streams.unsubscribe(stream_id).await?;
    
    // Verify cleanup
    assert!(!streams.subscribed.contains_key(&stream_id));
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_long_running_no_leak() {
    let mut network = setup_test_network();
    
    // Run for extended period with peer churn
    for _ in 0..1000 {
        let peer = generate_random_peer();
        network.add_peer(peer).await;
        
        if rand::random() {
            network.remove_random_peer().await;
        }
    }
    
    // Verify bounded memory usage
    let stats = network.get_memory_stats();
    assert!(stats.peer_count < 200); // Should stabilize
}
```

## Monitoring & Alerting

### Metrics to Track

```rust
// Prometheus-style metrics
memory_store_operations_total
memory_store_logs_total
address_book_peers_total
topic_streams_active_total
gossip_buffer_entries_total

// Alerts
memory_store_operations_total > 10000
address_book_peers_total > 1000
topic_streams_active_total > 100
```

### Health Check Endpoint

```rust
async fn health_check(state: State) -> impl IntoResponse {
    Json(json!({
        "memory_store": {
            "operations": state.store.stats().operation_count,
            "logs": state.store.stats().log_count,
        },
        "address_book": {
            "peers": state.address_book.peer_count().await,
        },
        "streams": {
            "active": state.streams.active_count(),
        }
    }))
}
```

## References

- [MEMORY_LEAK_ANALYSIS.md](./MEMORY_LEAK_ANALYSIS.md) - Detailed analysis
- [Rust Memory Safety](https://doc.rust-lang.org/book/ch04-00-understanding-ownership.html)
- [Tokio Best Practices](https://tokio.rs/tokio/topics/shutdown)
- [LRU Cache Crate](https://docs.rs/lru/latest/lru/)
