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

use datafusion::error::Result;
use datafusion::physical_plan::metrics::CustomMetricValue;
use parking_lot::Mutex;
use std::any::Any;
use std::fmt::{self, Debug, Display};
use std::sync::Arc;

/// Sends custom metric values from the executor to the scheduler as their own
/// type, so the scheduler aggregates and renders them as the operator does.
///
/// Register the same codec on the executor and the scheduler with
/// [`SessionConfigExt::with_ballista_custom_metric_codec`], next to the
/// physical extension codec that ships the operator itself. Values the codec
/// does not encode are sent as a [`SerializedCustomMetricValue`] snapshot.
///
/// [`SessionConfigExt::with_ballista_custom_metric_codec`]: crate::extension::SessionConfigExt::with_ballista_custom_metric_codec
pub trait CustomMetricCodec: Debug + Send + Sync {
    /// Encodes `value`, returning a tag naming its type and its bytes, or
    /// `None` if this codec does not know the type.
    fn try_encode(&self, value: &dyn CustomMetricValue) -> Option<(String, Vec<u8>)>;

    /// Rebuilds a value encoded by [`Self::try_encode`] with `type_tag`.
    fn try_decode(
        &self,
        type_tag: &str,
        encoded: &[u8],
    ) -> Result<Arc<dyn CustomMetricValue>>;
}

/// Snapshot of a [`MetricValue::Custom`] taken on the executor.
///
/// The executor's concrete `dyn CustomMetricValue` type does not exist on the
/// scheduler, so only its `Display` output and `as_usize()` cross the wire.
/// A single snapshot renders as the executor rendered it. Snapshots that are
/// aggregated together render as the number of values, since a total would
/// assume the type adds up. Register a [`CustomMetricCodec`] to aggregate them
/// as the executor's type does.
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
        // How the executor's type aggregates is not known here, so
        // aggregated snapshots are counted rather than given a total.
        if state.snapshots == 1 {
            f.write_str(&state.display)
        } else {
            write!(f, "{} values", state.snapshots)
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
    use crate::serde::scheduler::from_proto::metrics_set_from_proto;
    use crate::serde::scheduler::to_proto::metrics_set_to_proto;
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

    /// A metric that keeps the largest value, which a sum would get wrong.
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

    /// Encodes `PeakBytes` as its value in little-endian bytes.
    #[derive(Debug)]
    struct PeakBytesCodec;

    impl CustomMetricCodec for PeakBytesCodec {
        fn try_encode(&self, value: &dyn CustomMetricValue) -> Option<(String, Vec<u8>)> {
            let peak = value.as_any().downcast_ref::<PeakBytes>()?;
            let bytes = (peak.0.load(Ordering::Relaxed) as u64).to_le_bytes();
            Some(("peak_bytes".to_string(), bytes.to_vec()))
        }

        fn try_decode(
            &self,
            type_tag: &str,
            encoded: &[u8],
        ) -> Result<Arc<dyn CustomMetricValue>> {
            if type_tag != "peak_bytes" {
                return datafusion::common::internal_err!("unknown tag {type_tag}");
            }
            let bytes: [u8; 8] = encoded.try_into().map_err(|_| {
                datafusion::common::internal_datafusion_err!("bad peak_bytes")
            })?;
            Ok(Arc::new(PeakBytes(AtomicUsize::new(
                u64::from_le_bytes(bytes) as usize,
            ))))
        }
    }

    fn peaks() -> MetricsSet {
        let mut metrics = MetricsSet::new();
        for (partition, bytes) in [(0, 100), (1, 300), (2, 200)] {
            metrics.push(Arc::new(Metric::new(
                MetricValue::Custom {
                    name: Cow::Borrowed("peak"),
                    value: Arc::new(PeakBytes(AtomicUsize::new(bytes))),
                },
                Some(partition),
            )));
        }
        metrics
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
    fn aggregated_snapshots_render_as_a_count_without_a_codec() {
        let mut metrics = MetricsSet::new();
        metrics.push(Arc::new(Metric::new(millis(12), Some(0))));
        metrics.push(Arc::new(Metric::new(millis(30), Some(1))));

        let aggregated = round_trip(metrics).aggregate_by_name();
        let metric = aggregated.iter().next().unwrap();

        assert_eq!(metric.value().as_usize(), 42);
        assert_eq!(metric.value().to_string(), "2 values");
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

    #[test]
    fn codec_rebuilds_the_executors_type_on_the_scheduler() {
        let codec: &dyn CustomMetricCodec = &PeakBytesCodec;
        let proto = metrics_set_to_proto(&peaks(), Some(codec)).unwrap();
        let metrics = metrics_set_from_proto(proto, Some(codec)).unwrap();

        let first = metrics.iter().next().unwrap();
        let MetricValue::Custom { value, .. } = first.value() else {
            panic!("expected a custom metric");
        };
        assert!(value.as_any().downcast_ref::<PeakBytes>().is_some());

        // The type's own aggregate keeps the maximum, and its own Display
        // keeps the unit.
        let aggregated = metrics.aggregate_by_name();
        assert_eq!(
            aggregated.iter().next().unwrap().value().to_string(),
            "300 B"
        );
    }

    #[test]
    fn scheduler_without_the_codec_falls_back_to_snapshots() {
        let proto = metrics_set_to_proto(&peaks(), Some(&PeakBytesCodec)).unwrap();
        let metrics = metrics_set_from_proto(proto, None).unwrap();

        assert_eq!(metrics.iter().next().unwrap().value().to_string(), "100 B");
        let aggregated = metrics.aggregate_by_name();
        assert_eq!(
            aggregated.iter().next().unwrap().value().to_string(),
            "3 values"
        );
    }

    #[test]
    fn codec_skips_types_it_does_not_know() {
        let mut metrics = MetricsSet::new();
        metrics.push(Arc::new(Metric::new(millis(12), Some(0))));

        let proto = metrics_set_to_proto(&metrics, Some(&PeakBytesCodec)).unwrap();
        let Some(protobuf::operator_metric::Metric::Custom(custom)) =
            &proto.metrics[0].metric
        else {
            panic!("expected a custom metric");
        };
        assert!(custom.type_tag.is_empty());
        assert_eq!(custom.display, "12 ms");
    }
}
