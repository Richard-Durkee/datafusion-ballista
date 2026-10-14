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
/// Consumers are told apart by name. Reservations that share a name, such as
/// the ones an operator makes for each of its batches on one partition, count
/// as one consumer whose peak is the most they held together.
///
/// DataFusion's `TrackConsumersPool` forgets a consumer when it unregisters,
/// so by the time a task finishes it has nothing left to report. This pool
/// keeps every consumer's peak for as long as the task's runtime lives, so
/// the executor can name the operator that used the most memory when it
/// reports the task's memory usage.
#[derive(Debug)]
pub struct ConsumerPeaksPool {
    inner: Arc<dyn MemoryPool>,
    consumers: Mutex<HashMap<String, ConsumerPeak>>,
}

#[derive(Debug, Default)]
struct ConsumerPeak {
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

    /// Every consumer that reserved memory, with the most it held at once in
    /// bytes, largest first.
    pub fn consumer_peaks(&self) -> Vec<(String, usize)> {
        let mut peaks: Vec<_> = self
            .consumers
            .lock()
            .iter()
            .filter(|(_, consumer)| consumer.peak > 0)
            .map(|(name, consumer)| (name.clone(), consumer.peak))
            .collect();
        peaks.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        peaks
    }

    fn record_growth(&self, reservation: &MemoryReservation, additional: usize) {
        let mut consumers = self.consumers.lock();
        let tracked = consumers
            .entry(reservation.consumer().name().to_string())
            .or_default();
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
        // A reservation frees what it holds, through `shrink`, before it
        // unregisters, so there is nothing left to account for here.
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.record_growth(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        if let Some(tracked) =
            self.consumers.lock().get_mut(reservation.consumer().name())
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

    fn peaks(pool: &Arc<dyn MemoryPool>) -> Vec<(String, usize)> {
        pool.downcast_ref::<ConsumerPeaksPool>()
            .unwrap()
            .consumer_peaks()
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
        assert_eq!(
            peaks(&pool),
            vec![
                ("HashJoinInput[3]".to_string(), 600),
                ("ExternalSorter[0]".to_string(), 500),
            ]
        );
    }

    #[test]
    fn refused_growth_is_not_counted() {
        let pool = pool(100);
        let reservation = MemoryConsumer::new("a").register(&pool);
        reservation.try_grow(80).unwrap();
        assert!(reservation.try_grow(50).is_err());
        assert_eq!(peaks(&pool), vec![("a".to_string(), 80)]);
    }

    #[test]
    fn reservations_with_the_same_name_are_one_consumer() {
        let pool = pool(1000);
        let first = MemoryConsumer::new("RepartitionExec[0]").register(&pool);
        let second = MemoryConsumer::new("RepartitionExec[0]").register(&pool);
        first.grow(100);
        second.grow(150);
        drop(first);
        second.grow(50);

        assert_eq!(peaks(&pool), vec![("RepartitionExec[0]".to_string(), 250)]);
    }

    #[test]
    fn nothing_reserved_reports_no_consumer() {
        let pool = pool(100);
        let _idle = MemoryConsumer::new("idle").register(&pool);
        assert!(peaks(&pool).is_empty());
    }
}
