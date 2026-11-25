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

use crate::serde::protobuf::CustomMetricAggregation;
use datafusion::physical_plan::metrics::CustomMetricValue;
use parking_lot::Mutex;
use std::any::Any;
use std::fmt::{self, Debug, Display};
use std::sync::Arc;

/// Snapshot of a [`MetricValue::Custom`] taken on the executor.
///
/// The executor's concrete `dyn CustomMetricValue` type does not exist on the
/// scheduler, so its `Display` output, its `as_usize()` and how it aggregates
/// cross the wire. A single snapshot renders as the executor rendered it.
/// Aggregated snapshots render as their sum or maximum when the aggregation is
/// known, and otherwise as the number of values, since a guessed total could
/// be wrong.
///
/// [`MetricValue::Custom`]: datafusion::physical_plan::metrics::MetricValue::Custom
#[derive(Debug)]
pub struct SerializedCustomMetricValue {
    aggregation: CustomMetricAggregation,
    state: Mutex<State>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct State {
    /// The rendering shared by every snapshot, or `None` once two differ.
    display: Option<String>,
    value: usize,
    /// Number of snapshots aggregated into this value.
    snapshots: usize,
}

impl SerializedCustomMetricValue {
    /// Creates a snapshot from the executor's rendered value, `as_usize()` and
    /// aggregation.
    pub fn new(
        display: impl Into<String>,
        value: usize,
        aggregation: CustomMetricAggregation,
    ) -> Self {
        Self {
            aggregation,
            state: Mutex::new(State {
                display: Some(display.into()),
                value,
                snapshots: 1,
            }),
        }
    }

    fn snapshot(&self) -> State {
        self.state.lock().clone()
    }
}

/// Works out how `value` aggregates by aggregating it into an empty value
/// twice: a sum doubles, a maximum stays the same.
pub fn probe_aggregation(value: &Arc<dyn CustomMetricValue>) -> CustomMetricAggregation {
    let single = value.as_usize();
    if single == 0 {
        return CustomMetricAggregation::Unknown;
    }
    let probe = value.new_empty();
    probe.aggregate(Arc::clone(value));
    if probe.as_usize() != single {
        return CustomMetricAggregation::Unknown;
    }
    probe.aggregate(Arc::clone(value));
    match probe.as_usize() {
        twice if Some(twice) == single.checked_mul(2) => CustomMetricAggregation::Sum,
        same if same == single => CustomMetricAggregation::Max,
        _ => CustomMetricAggregation::Unknown,
    }
}

impl Display for SerializedCustomMetricValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock();
        match (state.snapshots, self.aggregation, &state.display) {
            (1, _, Some(display)) => f.write_str(display),
            (_, CustomMetricAggregation::Sum | CustomMetricAggregation::Max, _) => {
                write!(f, "{}", state.value)
            }
            (snapshots, _, _) => write!(f, "{snapshots} values"),
        }
    }
}

impl CustomMetricValue for SerializedCustomMetricValue {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(Self {
            aggregation: self.aggregation,
            state: Mutex::new(State {
                display: None,
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
                display: Some(other.to_string()),
                value: other.as_usize(),
                snapshots: 1,
            },
        };
        let mut state = self.state.lock();
        state.display = if state.snapshots == 0 {
            other.display
        } else if state.display == other.display {
            state.display.take()
        } else {
            None
        };
        state.value = match self.aggregation {
            CustomMetricAggregation::Max => state.value.max(other.value),
            _ => state.value.saturating_add(other.value),
        };
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
            std::ptr::eq(self, other)
                || (self.aggregation == other.aggregation
                    && self.snapshot() == other.snapshot())
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

    /// A metric that keeps the largest value, like a peak.
    #[derive(Debug, Default)]
    struct PeakBytes(AtomicUsize);

    impl Display for PeakBytes {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{} B", self.0.load(Ordering::Relaxed))
        }
    }

    impl CustomMetricValue for PeakBytes {
        fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
            Arc::new(Self::default())
        }

        fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
            self.0.fetch_max(other.as_usize(), Ordering::Relaxed);
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

    /// A metric with no meaningful number, so `as_usize()` keeps its default.
    #[derive(Debug)]
    struct Codec(&'static str);

    impl Display for Codec {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl CustomMetricValue for Codec {
        fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
            Arc::new(Self(""))
        }

        fn aggregate(&self, _other: Arc<dyn CustomMetricValue + 'static>) {}

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
            other.to_string() == self.0
        }
    }

    fn custom(
        name: &'static str,
        value: impl CustomMetricValue + 'static,
    ) -> MetricValue {
        MetricValue::Custom {
            name: Cow::Borrowed(name),
            value: Arc::new(value),
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
        let sum = CustomMetricAggregation::Sum;
        let a = SerializedCustomMetricValue::new("12 ms", 12, sum);
        let same: Arc<dyn CustomMetricValue> =
            Arc::new(SerializedCustomMetricValue::new("12 ms", 12, sum));
        let different: Arc<dyn CustomMetricValue> =
            Arc::new(SerializedCustomMetricValue::new("13 ms", 13, sum));

        assert!(a.is_eq(&same));
        assert!(!a.is_eq(&different));
    }

    #[test]
    fn probe_tells_sums_from_maximums() {
        let sum: Arc<dyn CustomMetricValue> = Arc::new(Millis(AtomicUsize::new(12)));
        let max: Arc<dyn CustomMetricValue> = Arc::new(PeakBytes(AtomicUsize::new(300)));
        let no_number: Arc<dyn CustomMetricValue> = Arc::new(Codec("zstd"));

        assert_eq!(probe_aggregation(&sum), CustomMetricAggregation::Sum);
        assert_eq!(probe_aggregation(&max), CustomMetricAggregation::Max);
        assert_eq!(
            probe_aggregation(&no_number),
            CustomMetricAggregation::Unknown
        );
    }

    #[test]
    fn aggregated_maximums_render_as_the_largest_value() {
        let mut metrics = MetricsSet::new();
        for (partition, bytes) in [(0, 100), (1, 300), (2, 200)] {
            metrics.push(Arc::new(Metric::new(
                custom("peak", PeakBytes(AtomicUsize::new(bytes))),
                Some(partition),
            )));
        }

        let aggregated = round_trip(metrics).aggregate_by_name();
        let metric = aggregated.iter().next().unwrap();

        assert_eq!(metric.value().as_usize(), 300);
        assert_eq!(metric.value().to_string(), "300");
    }

    #[test]
    fn aggregated_values_of_unknown_aggregation_render_as_a_count() {
        let mut metrics = MetricsSet::new();
        metrics.push(Arc::new(Metric::new(
            custom("codec", Codec("zstd")),
            Some(0),
        )));
        metrics.push(Arc::new(Metric::new(
            custom("codec", Codec("lz4")),
            Some(1),
        )));

        let aggregated = round_trip(metrics).aggregate_by_name();
        let metric = aggregated.iter().next().unwrap();

        assert_eq!(metric.value().to_string(), "2 values");
    }
}
