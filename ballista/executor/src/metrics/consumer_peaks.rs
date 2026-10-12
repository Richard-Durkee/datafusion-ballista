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

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use parking_lot::Mutex;

/// Wraps a task's memory pool and remembers the most each memory consumer
/// (an operator's reservation, such as `HashJoinInput[3]`) held at once,
/// including consumers that have since been dropped.
///
/// DataFusion's `TrackConsumersPool` forgets a consumer when it unregisters,
/// so by the time a task finishes it has nothing left to report. This pool
/// keeps every consumer's peak for as long as the task's runtime lives, so
/// the executor can name the operator that used the most memory when it
/// reports the task's memory usage.
#[derive(Debug)]
pub struct ConsumerPeaksPool {
    inner: Arc<dyn MemoryPool>,
    consumers: Mutex<HashMap<usize, ConsumerPeak>>,
}

#[derive(Debug)]
struct ConsumerPeak {
    name: String,
    reserved: usize,
    peak: usize,
}

impl ConsumerPeaksPool {
    /// Wraps `inner`, which actually grants and refuses reservations.
    pub fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            consumers: Mutex::new(HashMap::new()),
        }
    }

    /// The wrapped pool.
    pub fn inner(&self) -> &dyn MemoryPool {
        self.inner.as_ref()
    }

    /// The consumer that held the most memory at once, with that peak in
    /// bytes. `None` if no consumer has reserved anything.
    pub fn largest_consumer(&self) -> Option<(String, usize)> {
        self.consumers
            .lock()
            .values()
            .filter(|consumer| consumer.peak > 0)
            .max_by_key(|consumer| consumer.peak)
            .map(|consumer| (consumer.name.clone(), consumer.peak))
    }

    fn record_growth(&self, reservation: &MemoryReservation, additional: usize) {
        let consumer = reservation.consumer();
        let mut consumers = self.consumers.lock();
        let tracked = consumers
            .entry(consumer.id())
            .or_insert_with(|| ConsumerPeak {
                name: consumer.name().to_string(),
                reserved: 0,
                peak: 0,
            });
        tracked.reserved += additional;
        tracked.peak = tracked.peak.max(tracked.reserved);
    }
}

impl Display for ConsumerPeaksPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(inner_pool: {})", self.name(), self.inner)
    }
}

impl MemoryPool for ConsumerPeaksPool {
    fn name(&self) -> &str {
        "consumer_peaks"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
        // Keep the peak; only what is still held goes away.
        if let Some(tracked) = self.consumers.lock().get_mut(&consumer.id()) {
            tracked.reserved = 0;
        }
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.record_growth(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        if let Some(tracked) = self.consumers.lock().get_mut(&reservation.consumer().id())
        {
            tracked.reserved = tracked.reserved.saturating_sub(shrink);
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.record_growth(reservation, additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;

    fn pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(ConsumerPeaksPool::new(Arc::new(GreedyMemoryPool::new(
            limit,
        ))))
    }

    fn largest(pool: &Arc<dyn MemoryPool>) -> Option<(String, usize)> {
        pool.downcast_ref::<ConsumerPeaksPool>()
            .unwrap()
            .largest_consumer()
    }

    #[test]
    fn remembers_peaks_of_dropped_consumers() {
        let pool = pool(1000);
        let build = MemoryConsumer::new("HashJoinInput[3]").register(&pool);
        let sort = MemoryConsumer::new("ExternalSorter[0]").register(&pool);
        build.try_grow(600).unwrap();
        sort.grow(300);
        build.shrink(500);
        sort.grow(200);
        drop(build);
        drop(sort);

        assert_eq!(pool.reserved(), 0);
        assert_eq!(largest(&pool), Some(("HashJoinInput[3]".to_string(), 600)));
    }

    #[test]
    fn refused_growth_is_not_counted() {
        let pool = pool(100);
        let reservation = MemoryConsumer::new("a").register(&pool);
        reservation.try_grow(80).unwrap();
        assert!(reservation.try_grow(50).is_err());
        assert_eq!(largest(&pool), Some(("a".to_string(), 80)));
    }

    #[test]
    fn nothing_reserved_reports_no_consumer() {
        let pool = pool(100);
        let _idle = MemoryConsumer::new("idle").register(&pool);
        assert_eq!(largest(&pool), None);
    }
}
