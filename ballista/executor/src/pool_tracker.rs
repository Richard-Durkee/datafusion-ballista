// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Tracks the memory pools of an executor's running tasks.

use std::sync::{Arc, Weak};

use datafusion::execution::memory_pool::MemoryPool;
use parking_lot::Mutex;

/// Tracks the per-task memory pools an executor hands out, so their combined
/// reservation can be reported in heartbeats.
///
/// Each task gets its own pool (see `memory_pool_policy` in
/// `executor_process`), so no single pool knows how much memory the executor
/// as a whole has reserved. The tracker holds a weak reference to each pool:
/// a pool drops out once its task's runtime is dropped, without the task
/// having to unregister it.
#[derive(Debug)]
pub struct TaskPoolTracker {
    capacity: u64,
    pools: Mutex<Vec<Weak<dyn MemoryPool>>>,
}

impl TaskPoolTracker {
    /// Creates a tracker for an executor whose task pools share a budget of
    /// `capacity` bytes.
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity,
            pools: Mutex::new(Vec::new()),
        }
    }

    /// Starts tracking `pool`, which belongs to a task that is about to run.
    pub fn track(&self, pool: &Arc<dyn MemoryPool>) {
        let mut pools = self.pools.lock();
        pools.retain(|pool| pool.strong_count() > 0);
        pools.push(Arc::downgrade(pool));
    }

    /// The executor's total memory budget, in bytes.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Bytes currently reserved across the pools of all running tasks.
    pub fn reserved(&self) -> u64 {
        let mut pools = self.pools.lock();
        pools.retain(|pool| pool.strong_count() > 0);
        pools
            .iter()
            .filter_map(Weak::upgrade)
            .map(|pool| pool.reserved() as u64)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::{FairSpillPool, MemoryConsumer};

    fn pool(size: usize) -> Arc<dyn MemoryPool> {
        Arc::new(FairSpillPool::new(size))
    }

    #[test]
    fn sums_reservations_across_tracked_pools() {
        let tracker = TaskPoolTracker::new(4096);
        let (a, b) = (pool(2048), pool(2048));
        tracker.track(&a);
        tracker.track(&b);

        let ra = MemoryConsumer::new("a").register(&a);
        let rb = MemoryConsumer::new("b").register(&b);
        ra.try_grow(1000).unwrap();
        rb.try_grow(300).unwrap();

        assert_eq!(tracker.capacity(), 4096);
        assert_eq!(tracker.reserved(), 1300);

        drop(rb);
        assert_eq!(tracker.reserved(), 1000);
    }

    #[test]
    fn forgets_pools_of_finished_tasks() {
        let tracker = TaskPoolTracker::new(4096);
        let finished = pool(2048);
        tracker.track(&finished);
        let reservation = MemoryConsumer::new("r").register(&finished);
        reservation.try_grow(500).unwrap();
        assert_eq!(tracker.reserved(), 500);

        // The task ends: its reservations and runtime (the pool) are dropped.
        drop(reservation);
        drop(finished);
        assert_eq!(tracker.reserved(), 0);
        assert!(tracker.pools.lock().is_empty());
    }
}
