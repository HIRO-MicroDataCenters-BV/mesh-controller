# Memory Leak Analysis for Patch Directory Crates

## Executive Summary

This document provides a comprehensive analysis of potential memory leak issues found in the patched crates within the `patch/` directory. The analysis covers quinn, iroh-gossip, and p2panda library crates used by the mesh-controller project.

## Critical Memory Leak Issues

### 1. **p2panda-net: AddressBook Unbounded Growth**

**Location:** `patch/p2panda/p2panda-net/src/engine/address_book.rs`

**Severity:** HIGH

**Issue Description:**
The `AddressBook` struct maintains two HashMaps that grow indefinitely without any cleanup mechanism:
- `known_peer_topic_ids: HashMap<PublicKey, HashSet<[u8; 32]>>`
- `known_peer_addresses: HashMap<PublicKey, HashSet<NodeAddress>>`

**Code:**
```rust
pub async fn add_peer(&mut self, node_addr: NodeAddress) {
    let public_key = node_addr.public_key;
    self.add_topic_id(public_key, self.network_id).await;
    
    let mut inner = self.inner.write().await;
    inner
        .known_peer_addresses
        .entry(public_key)
        .or_default()
        .insert(node_addr);
}

pub async fn add_topic_id(&mut self, public_key: PublicKey, topic_id: [u8; 32]) {
    let mut inner = self.inner.write().await;
    inner
        .known_peer_topic_ids
        .entry(public_key)
        .and_modify(|known_topics| {
            known_topics.insert(topic_id);
        })
        .or_insert({
            let mut topics = HashSet::new();
            topics.insert(topic_id);
            topics
        });
}
```

**Problem:**
- No method exists to remove stale or disconnected peers
- No TTL (Time To Live) mechanism for peer entries
- No maximum capacity limit
- In a long-running application with peer churn, this will continuously grow

**Impact:**
- Memory consumption grows linearly with the number of unique peers ever encountered
- No reclamation of memory from disconnected or inactive peers
- Can eventually lead to OOM (Out Of Memory) errors in long-running deployments

**Recommendations:**
1. Implement a `remove_peer()` method to clean up disconnected peers
2. Add TTL-based expiration for peer entries
3. Implement LRU (Least Recently Used) cache with maximum capacity
4. Add periodic cleanup task to remove stale entries
5. Track peer connection state and remove on disconnect

### 2. **p2panda-net: GossipBuffer Counter Leak**

**Location:** `patch/p2panda/p2panda-net/src/engine/gossip_buffer.rs`

**Severity:** MEDIUM

**Issue Description:**
The `GossipBuffer` maintains counters that may never be cleaned up even when they reach zero:

```rust
pub struct GossipBuffer {
    buffers: HashMap<(PublicKey, [u8; 32]), Vec<Vec<u8>>>,
    counters: HashMap<(PublicKey, [u8; 32]), usize>,
}
```

**Problem:**
- Counters are never removed from the `counters` HashMap, even when they reach zero
- The `buffers` HashMap only gets cleaned via `drain()`, but counters remain
- Each unique (PublicKey, topic_id) pair creates a permanent counter entry

**Code Analysis:**
```rust
pub fn unlock(&mut self, peer: PublicKey, topic_id: [u8; 32]) -> Option<usize> {
    match self.counters.get_mut(&(peer, topic_id)) {
        Some(counter) if *counter > 0 => {
            *counter -= 1;
            // Counter stays in HashMap even when reaching 0
            Some(*counter)
        }
        _ => None
    }
}

pub fn drain(&mut self, peer: PublicKey, topic_id: [u8; 32]) -> Option<Vec<Vec<u8>>> {
    self.buffers.remove(&(peer, topic_id))
    // Counter is NOT removed here
}
```

**Impact:**
- Memory leak grows with the number of unique (peer, topic) combinations
- Each entry is small but accumulates over time
- Prevents proper cleanup even after peers disconnect

**Recommendations:**
1. Remove counter entries when they reach zero
2. Clean up counters in the `drain()` method
3. Add a cleanup method to remove entries where counter == 0 and buffer is empty

### 3. **p2panda-net: TopicStreams Subscription Leak**

**Location:** `patch/p2panda/p2panda-net/src/engine/topic_streams.rs`

**Severity:** MEDIUM-HIGH

**Issue Description:**
The `TopicStreams` struct manages multiple HashMaps for stream subscriptions without proper cleanup when streams are closed:

```rust
pub struct TopicStreams<T> {
    address_book: AddressBook,
    gossip_actor_tx: mpsc::Sender<ToGossipActor>,
    gossip_buffer: GossipBuffer,
    gossip_joined: Arc<RwLock<HashSet<[u8; 32]>>>,
    gossip_pending: HashMap<[u8; 32], oneshot::Sender<()>>,
    next_stream_id: usize,
    subscribed: HashMap<TopicStreamId, TopicStream<T>>,
    topic_id_to_stream: HashMap<[u8; 32], Vec<TopicStreamId>>,
    topic_to_stream: HashMap<T, Vec<TopicStreamId>>,
    sync_actor_tx: Option<mpsc::Sender<ToSyncActor<T>>>,
}
```

**Problem:**
- No `unsubscribe()` method exists to remove closed streams
- Stream IDs continuously increment via `next_stream_id += 1`
- Multiple mappings (subscribed, topic_id_to_stream, topic_to_stream) are never cleaned
- When mpsc channels close, the stream data remains in memory

**Impact:**
- Each subscription permanently allocates memory
- Long-running applications with dynamic subscriptions will leak memory
- Zombie stream entries remain after channels close

**Recommendations:**
1. Implement an `unsubscribe()` or `remove_stream()` method
2. Detect closed mpsc channels and automatically clean up
3. Add Drop handler to clean up stream entries
4. Consider using weak references for some stream mappings

### 4. **p2panda-store: MemoryStore Unbounded Growth**

**Location:** `patch/p2panda/p2panda-store/src/memory.rs`

**Severity:** MEDIUM (by design, but risky)

**Issue Description:**
The in-memory store grows without bounds:

```rust
pub struct InnerMemoryStore<L, E> {
    operations: HashMap<Hash, StoredOperation<L, E>>,
    logs: HashMap<(PublicKey, L), BTreeSet<LogMeta>>,
}
```

**Problem:**
- While `delete_operation()` and `delete_operations()` methods exist, they must be explicitly called
- No automatic cleanup or capacity limits
- No LRU eviction policy
- Designed for testing/memory backend but used in production

**Current Mitigation:**
The code does provide deletion methods, but they are not automatically invoked:
```rust
async fn delete_operation(&mut self, hash: Hash) -> Result<bool, Self::Error>
async fn delete_operations(&mut self, public_key: &PublicKey, log_id: &L, before: SeqNum) -> Result<bool, Self::Error>
```

**Impact:**
- If deletion methods are not called regularly, memory grows indefinitely
- No warning when memory usage is high
- Can lead to OOM in production if used as persistent store

**Recommendations:**
1. Document clearly that this is for testing only, or
2. Implement automatic cleanup policies (LRU, TTL, max capacity)
3. Add memory usage monitoring and warnings
4. Consider adding a periodic cleanup task
5. Provide configuration for maximum memory limits

### 5. **quinn-proto: Incoming Connection Buffer Leak**

**Location:** `patch/quinn/quinn-proto/src/endpoint.rs`

**Severity:** MEDIUM (well-documented, but critical if misused)

**Issue Description:**
The `Incoming` struct contains a warning mechanism for improper drops:

```rust
pub struct Incoming {
    received_at: Instant,
    addresses: FourTuple,
    ecn: Option<EcnCodepoint>,
    packet: InitialPacket,
    rest: Option<BytesMut>,
    crypto: Keys,
    token: IncomingToken,
    incoming_idx: usize,
    improper_drop_warner: IncomingImproperDropWarner,
}

impl Drop for IncomingImproperDropWarner {
    fn drop(&mut self) {
        warn!("quinn_proto::Incoming dropped without passing to Endpoint::accept/refuse/retry/ignore \
               (may cause memory leak and eventual inability to accept new connections)");
    }
}
```

**Problem:**
- If `Incoming` is dropped without calling `accept()`, `refuse()`, `retry()`, or `ignore()`, the associated buffer in `Endpoint.incoming_buffers` is not cleaned up
- This warning is logged but the leak still happens
- The high-level quinn API (not proto) has a Drop impl that calls refuse(), but errors in code paths could still leak

**Code Path:**
```rust
// In endpoint.rs - low level proto
struct Endpoint {
    incoming_buffers: Slab<IncomingBuffer>,
    all_incoming_buffers_total_bytes: u64,
    // ...
}

pub fn accept(&mut self, mut incoming: Incoming, ...) {
    incoming.improper_drop_warner.dismiss();
    let incoming_buffer = self.incoming_buffers.remove(incoming.incoming_idx);
    self.all_incoming_buffers_total_bytes -= incoming_buffer.total_bytes;
    // ...
}
```

**Impact:**
- Leaked buffers accumulate in `incoming_buffers` Slab
- Memory is never reclaimed
- Eventually leads to inability to accept new connections
- Counter `all_incoming_buffers_total_bytes` becomes inaccurate

**Current Mitigation:**
The high-level quinn API provides implicit cleanup:
```rust
// In quinn/src/incoming.rs
impl Drop for Incoming {
    fn drop(&mut self) {
        if let Some(state) = self.0.take() {
            state.endpoint.refuse(state.inner);
        }
    }
}
```

**Recommendations:**
1. Users should use the high-level quinn API, not proto directly
2. Add debug assertions to detect buffer leaks in development
3. Consider adding automatic cleanup for old buffers (timeout-based)
4. Add metrics to monitor incoming_buffers size
5. Document this hazard prominently

## Medium Priority Issues

### 6. **Detached Task Spawning Without Tracking**

**Locations:**
- `patch/quinn/quinn/src/runtime/async_io.rs`
- `patch/p2panda/p2panda-blobs/src/download.rs`
- `patch/p2panda/p2panda-blobs/src/import.rs`

**Issue Description:**
Several locations spawn detached tasks that run in the background without tracking:

```rust
// quinn async_io
fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
    ::smol::spawn(future).detach();
}

// p2panda blobs
pool_handle.spawn_detached(move || async move {
    match download_queued(network, &downloader, hash_and_format, progress.clone()).await {
        Ok(stats) => { /* ... */ }
        Err(err) => { /* ... */ }
    }
    // No tracking, cannot cancel or wait for completion
});
```

**Problem:**
- Detached tasks cannot be cancelled
- No way to wait for completion during shutdown
- Errors in detached tasks may go unnoticed
- Resources held by tasks may not be released promptly

**Impact:**
- Graceful shutdown becomes difficult
- Tasks may continue running after application shutdown
- Resource leaks if tasks hold connections, file handles, etc.
- Test flakiness due to lingering tasks

**Recommendations:**
1. Track spawned tasks in a JoinSet or similar structure
2. Implement graceful shutdown that waits for or cancels tasks
3. Use cancellation tokens (e.g., tokio-util's CancellationToken)
4. Add timeout for task completion during shutdown
5. Log errors from background tasks prominently

### 7. **EventStream Receiver Cleanup Timing**

**Location:** `patch/iroh-gossip/src/net.rs`

**Issue Description:**
The EventStream Drop implementation tries to notify an actor but may fail:

```rust
impl Drop for EventStream {
    fn drop(&mut self) {
        if let Err(e) = self.to_actor_tx.try_send(ToActor::ReceiverGone {
            topic: self.topic,
            receiver_id: self.receiver_id,
        }) {
            match e {
                mpsc::error::TrySendError::Full(msg) => {
                    // Spawns cleanup task if channel is full
                    if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        let to_actor_tx = self.to_actor_tx.clone();
                        handle.spawn(async move {
                            let _ = to_actor_tx.send(msg).await;
                        });
                    }
                }
                mpsc::error::TrySendError::Closed(_) => {
                    // Actor already gone, no cleanup possible
                }
            }
        }
    }
}
```

**Problem:**
- If `try_send` fails because channel is full, spawns an untracked task
- If no runtime is available, cleanup message is silently dropped
- If actor is already shut down, cleanup cannot happen
- Race condition if actor shuts down between try_send and spawn

**Impact:**
- Actor may not clean up receiver state
- Memory leak in actor's receiver tracking
- Potential for duplicate receivers if receiver_id is reused

**Recommendations:**
1. Ensure actor stays alive until all EventStreams are dropped
2. Use bounded channels with sufficient capacity
3. Add logging when cleanup fails
4. Consider using weak references for receiver tracking in actor

## Low Priority Issues

### 8. **p2panda-encryption: TODO Comment About Memory Usage**

**Location:** `patch/p2panda/p2panda-encryption/src/data_scheme/test_utils/ordering.rs:132`

**Issue:**
```rust
// TODO: We keep all messages in memory currently which is bad. This needs a persistence
```

This is in test utilities, so lower priority, but indicates awareness of memory issues.

**Recommendation:**
Address the TODO or clearly mark the code as test-only.

## General Recommendations

### Monitoring and Observability

1. **Add Memory Metrics:**
   - Track size of AddressBook, TopicStreams, GossipBuffer
   - Monitor incoming_buffers size in quinn
   - Alert on unbounded growth

2. **Logging:**
   - Log when collections exceed thresholds
   - Log peer additions/removals
   - Log stream subscription/unsubscription

3. **Health Checks:**
   - Add health endpoints that report memory usage
   - Include collection sizes in health checks
   - Implement memory pressure detection

### Testing

1. **Leak Detection Tests:**
   - Add tests that create and destroy many peers/streams
   - Verify memory is reclaimed after cleanup
   - Use tools like valgrind or ASAN in CI

2. **Long-Running Tests:**
   - Soak tests that run for extended periods
   - Monitor memory growth over time
   - Test graceful shutdown under load

3. **Fuzzing:**
   - Fuzz connection handling in quinn
   - Fuzz peer management in p2panda
   - Test error paths that might skip cleanup

### Code Quality

1. **RAII Patterns:**
   - Use Drop implementations for automatic cleanup
   - Avoid manual resource management where possible
   - Use guard types to ensure cleanup

2. **Bounded Collections:**
   - Replace unbounded HashMaps with LRU caches where appropriate
   - Add capacity limits with clear overflow behavior
   - Consider using ring buffers for fixed-size histories

3. **Documentation:**
   - Document memory usage characteristics
   - Clarify ownership and lifetime expectations
   - Add examples of proper cleanup

## Conclusion

The most critical issues are:

1. **AddressBook unbounded growth** - needs immediate attention for production use
2. **TopicStreams subscription leak** - requires unsubscribe implementation
3. **GossipBuffer counter leak** - simple fix, should be addressed
4. **Incoming buffer leak** - already mitigated by high-level API but needs monitoring

The codebase shows good awareness of memory issues (warning mechanisms, Drop implementations) but lacks systematic cleanup strategies for long-running operation. Implementing the recommendations above will significantly improve memory safety and production readiness.

## Priority Action Items

**High Priority:**
1. Implement AddressBook cleanup mechanism
2. Add TopicStreams unsubscribe method
3. Fix GossipBuffer counter cleanup
4. Add memory usage monitoring

**Medium Priority:**
5. Implement task tracking for detached spawns
6. Add LRU/TTL policies to MemoryStore
7. Improve EventStream cleanup reliability

**Low Priority:**
8. Add comprehensive memory leak tests
9. Improve documentation
10. Add memory pressure handling
