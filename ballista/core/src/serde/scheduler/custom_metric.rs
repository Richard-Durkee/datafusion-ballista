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

//! A [`CustomMetricValue`] that survives the trip from executor to scheduler.

use datafusion::physical_plan::metrics::CustomMetricValue;
use parking_lot::Mutex;
use std::any::Any;
use std::fmt::{self, Debug, Display};
use std::sync::Arc;

/// Snapshot of a [`MetricValue::Custom`] taken on the executor.
///
/// The executor's concrete `dyn CustomMetricValue` type does not exist on the
/// scheduler, so only its `Display` output and `as_usize()` cross the wire.
/// A single snapshot renders as the executor rendered it. Snapshots that are
/// aggregated together render as the sum of their `as_usize()` values, since
/// the rendered strings cannot be merged.
///
/// [`MetricValue::Custom`]: datafusion::physical_plan::metrics::MetricValue::Custom
#[derive(Debug)]
pub struct SerializedCustomMetricValue {
    state: Mutex<State>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct State {
    display: String,
    value: usize,
    /// Number of snapshots aggregated into this value.
    snapshots: usize,
}

impl SerializedCustomMetricValue {
    /// Creates a snapshot from the executor's rendered value and `as_usize()`.
    pub fn new(display: impl Into<String>, value: usize) -> Self {
        Self {
            state: Mutex::new(State {
                display: display.into(),
                value,
                snapshots: 1,
            }),
        }
    }

    fn snapshot(&self) -> State {
        self.state.lock().clone()
    }
}

impl Display for SerializedCustomMetricValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock();
        if state.snapshots == 1 {
            f.write_str(&state.display)
        } else {
            write!(f, "{}", state.value)
        }
    }
}

impl CustomMetricValue for SerializedCustomMetricValue {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(Self {
            state: Mutex::new(State {
                display: String::new(),
                value: 0,
                snapshots: 0,
            }),
        })
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
        let other = match other.as_any().downcast_ref::<Self>() {
            // Aggregating a value into itself would deadlock on its own lock.
            Some(other) if std::ptr::eq(self, other) => return,
            Some(other) => other.snapshot(),
            None => State {
                display: other.to_string(),
                value: other.as_usize(),
                snapshots: 1,
            },
        };
        let mut state = self.state.lock();
        if state.snapshots == 0 {
            state.display = other.display;
        }
        state.value = state.value.saturating_add(other.value);
        state.snapshots += other.snapshots;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_usize(&self) -> usize {
        self.state.lock().value
    }

    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        other.as_any().downcast_ref::<Self>().is_some_and(|other| {
            std::ptr::eq(self, other) || self.snapshot() == other.snapshot()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::protobuf;
    use datafusion::physical_plan::Metric;
    use datafusion::physical_plan::metrics::{Count, MetricValue, MetricsSet};
    use std::borrow::Cow;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Stands in for a third-party operator's own metric type.
    #[derive(Debug, Default)]
    struct Millis(AtomicUsize);

    impl Display for Millis {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{} ms", self.0.load(Ordering::Relaxed))
        }
    }

    impl CustomMetricValue for Millis {
        fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
            Arc::new(Self::default())
        }

        fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
            self.0.fetch_add(other.as_usize(), Ordering::Relaxed);
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_usize(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }

        fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
            other.as_usize() == self.as_usize()
        }
    }

    fn millis(value: usize) -> MetricValue {
        MetricValue::Custom {
            name: Cow::Borrowed("io_wait"),
            value: Arc::new(Millis(AtomicUsize::new(value))),
        }
    }

    fn round_trip(metrics: MetricsSet) -> MetricsSet {
        let proto: protobuf::OperatorMetricsSet = metrics.try_into().unwrap();
        proto.try_into().unwrap()
    }

    #[test]
    fn custom_metric_does_not_drop_the_operators_other_metrics() {
        let rows = Count::new();
        rows.add(7);
        let mut metrics = MetricsSet::new();
        metrics.push(Arc::new(Metric::new(
            MetricValue::OutputRows(rows),
            Some(0),
        )));
        metrics.push(Arc::new(Metric::new(millis(12), Some(0))));

        let metrics = round_trip(metrics);

        assert_eq!(metrics.output_rows(), Some(7));
        let custom = metrics.sum_by_name("io_wait").unwrap();
        assert_eq!(custom.as_usize(), 12);
    }

    #[test]
    fn single_snapshot_renders_as_the_executor_rendered_it() {
        let mut metrics = MetricsSet::new();
        metrics.push(Arc::new(Metric::new(millis(12), Some(3))));

        let metrics = round_trip(metrics);
        let metric = metrics.iter().next().unwrap();

        assert_eq!(metric.partition(), Some(3));
        assert_eq!(metric.value().name(), "io_wait");
        assert_eq!(metric.value().to_string(), "12 ms");
        assert_eq!(metric.value().as_usize(), 12);
    }

    #[test]
    fn aggregated_snapshots_render_as_their_sum() {
        let mut metrics = MetricsSet::new();
        metrics.push(Arc::new(Metric::new(millis(12), Some(0))));
        metrics.push(Arc::new(Metric::new(millis(30), Some(1))));

        let aggregated = round_trip(metrics).aggregate_by_name();
        let metric = aggregated.iter().next().unwrap();

        assert_eq!(metric.value().as_usize(), 42);
        assert_eq!(metric.value().to_string(), "42");
    }

    #[test]
    fn aggregating_one_snapshot_keeps_its_rendering() {
        let mut metrics = MetricsSet::new();
        metrics.push(Arc::new(Metric::new(millis(12), Some(0))));

        let aggregated = round_trip(metrics).aggregate_by_name();
        let metric = aggregated.iter().next().unwrap();

        assert_eq!(metric.value().to_string(), "12 ms");
    }

    #[test]
    fn snapshots_compare_by_content() {
        let a = SerializedCustomMetricValue::new("12 ms", 12);
        let same: Arc<dyn CustomMetricValue> =
            Arc::new(SerializedCustomMetricValue::new("12 ms", 12));
        let different: Arc<dyn CustomMetricValue> =
            Arc::new(SerializedCustomMetricValue::new("13 ms", 13));

        assert!(a.is_eq(&same));
        assert!(!a.is_eq(&different));
    }
}
