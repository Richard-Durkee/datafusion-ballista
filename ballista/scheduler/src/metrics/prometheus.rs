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

use crate::metrics::SchedulerMetricsCollector;
use ballista_core::JobId;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf::TaskMemoryUsage;

use once_cell::sync::OnceCell;
use prometheus::{
    Counter, Gauge, Histogram, Registry, register_counter_with_registry,
    register_gauge_with_registry, register_histogram_with_registry,
};
use prometheus::{Encoder, TextEncoder};
use std::sync::Arc;

static COLLECTOR: OnceCell<Arc<dyn SchedulerMetricsCollector>> = OnceCell::new();

/// SchedulerMetricsCollector implementation based on Prometheus. By default this will track
/// 7 metrics:
/// *job_exec_time_seconds* - Histogram of successful job execution time in seconds
/// *planning_time_ms* - Histogram of job planning time in milliseconds
/// *job_failed_total* - Counter of failed jobs
/// *job_cancelled_total* - Counter of cancelled jobs
/// *job_completed_total* - Counter of completed jobs
/// *job_submitted_total* - Counter of submitted jobs
/// *pending_task_queue_size* - Number of pending tasks
///
/// and, for tasks that ran with a bounded memory pool:
/// *ballista_task_memory_pool_peak_bytes* - Histogram of each task's peak pool reservation
/// *ballista_task_memory_pool_utilization* - Histogram of each task's peak reservation as a
///   fraction of its pool size
/// *ballista_task_memory_exhausted_total* - Counter of tasks that failed because their pool
///   refused a reservation
pub struct PrometheusMetricsCollector {
    execution_time: Histogram,
    planning_time: Histogram,
    failed: Counter,
    cancelled: Counter,
    completed: Counter,
    submitted: Counter,
    pending_queue_size: Gauge,
    task_memory_peak: Histogram,
    task_memory_utilization: Histogram,
    task_memory_exhausted: Counter,
}

impl PrometheusMetricsCollector {
    /// Creates a new PrometheusMetricsCollector instance.
    pub fn new(registry: &Registry) -> Result<Self> {
        let execution_time = register_histogram_with_registry!(
            "job_exec_time_seconds",
            "Histogram of successful job execution time in seconds",
            vec![0.5_f64, 1_f64, 5_f64, 30_f64, 60_f64],
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let planning_time = register_histogram_with_registry!(
            "planning_time_ms",
            "Histogram of job planning time in milliseconds",
            vec![1.0_f64, 5.0_f64, 25.0_f64, 100.0_f64, 500.0_f64],
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let failed = register_counter_with_registry!(
            "job_failed_total",
            "Counter of failed jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let cancelled = register_counter_with_registry!(
            "job_cancelled_total",
            "Counter of cancelled jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let completed = register_counter_with_registry!(
            "job_completed_total",
            "Counter of completed jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let submitted = register_counter_with_registry!(
            "job_submitted_total",
            "Counter of submitted jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let pending_queue_size = register_gauge_with_registry!(
            "pending_task_queue_size",
            "Number of pending tasks",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let task_memory_peak = register_histogram_with_registry!(
            "ballista_task_memory_pool_peak_bytes",
            "Histogram of each task's peak memory pool reservation in bytes",
            // 1 MiB to 16 GiB in powers of 4.
            prometheus::exponential_buckets(1024.0 * 1024.0, 4.0, 8).map_err(|e| {
                BallistaError::Internal(format!("Error creating buckets: {e:?}"))
            })?,
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let task_memory_utilization = register_histogram_with_registry!(
            "ballista_task_memory_pool_utilization",
            "Histogram of each task's peak memory pool reservation as a fraction of its pool size",
            vec![0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 1.0],
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let task_memory_exhausted = register_counter_with_registry!(
            "ballista_task_memory_exhausted_total",
            "Counter of tasks that failed because their memory pool refused a reservation",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        Ok(Self {
            execution_time,
            planning_time,
            failed,
            cancelled,
            completed,
            submitted,
            pending_queue_size,
            task_memory_peak,
            task_memory_utilization,
            task_memory_exhausted,
        })
    }

    /// Returns the current global prometheus collector.
    pub fn current() -> Result<Arc<dyn SchedulerMetricsCollector>> {
        COLLECTOR
            .get_or_try_init(|| {
                let collector = Self::new(::prometheus::default_registry())?;

                Ok(Arc::new(collector) as Arc<dyn SchedulerMetricsCollector>)
            })
            .cloned()
    }
}

impl SchedulerMetricsCollector for PrometheusMetricsCollector {
    fn record_submitted(&self, _job_id: &JobId, queued_at: u64, submitted_at: u64) {
        self.submitted.inc();
        self.planning_time
            .observe((submitted_at - queued_at) as f64);
    }

    fn record_completed(&self, _job_id: &JobId, queued_at: u64, completed_at: u64) {
        self.completed.inc();
        self.execution_time
            .observe((completed_at - queued_at) as f64 / 1000_f64)
    }

    fn record_failed(&self, _job_id: &JobId, _queued_at: u64, _failed_at: u64) {
        self.failed.inc()
    }

    fn record_cancelled(&self, _job_id: &JobId) {
        self.cancelled.inc();
    }

    fn set_pending_tasks_queue_size(&self, value: u64) {
        self.pending_queue_size.set(value as f64);
    }

    fn record_task_memory(&self, usage: &TaskMemoryUsage) {
        self.task_memory_peak.observe(usage.pool_peak_bytes as f64);
        if usage.pool_limit_bytes > 0 {
            self.task_memory_utilization
                .observe(usage.pool_peak_bytes as f64 / usage.pool_limit_bytes as f64);
        }
    }

    fn record_task_memory_exhausted(&self) {
        self.task_memory_exhausted.inc();
    }

    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
        let encoder = TextEncoder::new();

        let metric_families = prometheus::gather();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).map_err(|e| {
            BallistaError::Internal(format!("Error encoding prometheus metrics: {e:?}"))
        })?;

        Ok(Some((buffer, encoder.format_type().to_owned())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_task_memory_metrics() -> Result<()> {
        let registry = Registry::new();
        let collector = PrometheusMetricsCollector::new(&registry)?;
        collector.record_task_memory(&TaskMemoryUsage {
            pool_limit_bytes: 1024,
            pool_peak_bytes: 512,
        });
        // A zero limit can't produce a utilization, but the peak is still kept.
        collector.record_task_memory(&TaskMemoryUsage {
            pool_limit_bytes: 0,
            pool_peak_bytes: 256,
        });
        collector.record_task_memory_exhausted();

        let families = registry.gather();
        let family = |name: &str| {
            families
                .iter()
                .find(|f| f.name() == name)
                .unwrap_or_else(|| panic!("{name} not registered"))
                .get_metric()[0]
                .clone()
        };
        let peak = family("ballista_task_memory_pool_peak_bytes");
        assert_eq!(peak.get_histogram().get_sample_count(), 2);
        assert_eq!(peak.get_histogram().get_sample_sum(), 768.0);
        let utilization = family("ballista_task_memory_pool_utilization");
        assert_eq!(utilization.get_histogram().get_sample_count(), 1);
        assert_eq!(utilization.get_histogram().get_sample_sum(), 0.5);
        let exhausted = family("ballista_task_memory_exhausted_total");
        assert_eq!(exhausted.get_counter().value(), 1.0);
        Ok(())
    }
}
