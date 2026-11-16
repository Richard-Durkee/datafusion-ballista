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

//! Implementation of ballista physical plan display with metrics. See
//! [`datafusion::physical_plan::display`] for examples of how to
//! format

use ballista_core::JobId;
use ballista_core::utils::collect_plan_metrics;
use datafusion::logical_expr::{StringifiedPlan, ToStringifiedPlan};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, ExecutionPlanVisitor, accept, displayable,
};
use log::{debug, error};
use std::fmt;

/// Merges executor-collected stage metrics into the plan's metric slots.
/// Returns None if the lengths don't match, which indicates a mismatch between two collections of metrics
fn merge_stage_metrics(
    plan: &dyn ExecutionPlan,
    stage_metrics: &[MetricsSet],
) -> Option<Vec<MetricsSet>> {
    let mut plan_metrics = collect_plan_metrics(plan);
    if plan_metrics.len() != stage_metrics.len() {
        return None;
    }
    plan_metrics
        .iter_mut()
        .zip(stage_metrics)
        .for_each(|(plan_metric, stage_metric)| {
            stage_metric
                .iter()
                .for_each(|s| plan_metric.push(s.clone()));
        });
    Some(plan_metrics)
}

/// Prints the physical plan for a completed stage with its aggregated metrics.
pub fn print_stage_metrics(
    job_id: &JobId,
    stage_id: usize,
    plan: &dyn ExecutionPlan,
    stage_metrics: &[MetricsSet],
) {
    // The plan_metrics collected here is a snapshot clone from the plan metrics.
    // They are all empty now and need to combine with the stage metrics in the ExecutionStages
    match merge_stage_metrics(plan, stage_metrics) {
        Some(plan_metrics) => debug!(
            "\n=== [{}/{}] Stage finished, physical plan with metrics ===\n{}",
            job_id,
            stage_id,
            DisplayableBallistaExecutionPlan::new(plan, &plan_metrics).indent()
        ),
        None => error!(
            "Fail to combine stage metrics to plan for stage [{}/{}], \
             plan metrics array size {} does not equal stage metrics array size {}",
            job_id,
            stage_id,
            collect_plan_metrics(plan).len(),
            stage_metrics.len()
        ),
    }
}

/// Formats the plan for a completed stage with its aggregated metrics.
pub fn format_stage_metrics(
    plan: &dyn ExecutionPlan,
    stage_metrics: &[MetricsSet],
) -> String {
    match merge_stage_metrics(plan, stage_metrics) {
        Some(plan_metrics) => DisplayableBallistaExecutionPlan::new(plan, &plan_metrics)
            .indent()
            .to_string(),
        None => displayable(plan).indent(false).to_string(),
    }
}

/// Wraps an `ExecutionPlan` to display it with aggregated metrics.
///
/// The metrics must be collected in the same order as nodes are visited during display.
pub struct DisplayableBallistaExecutionPlan<'a> {
    /// The execution plan to display.
    inner: &'a dyn ExecutionPlan,
    /// Metrics for each node in the plan tree.
    metrics: &'a Vec<MetricsSet>,
}

impl<'a> DisplayableBallistaExecutionPlan<'a> {
    /// Creates a wrapper around an [`ExecutionPlan`] for pretty printing with metrics.
    pub fn new(inner: &'a dyn ExecutionPlan, metrics: &'a Vec<MetricsSet>) -> Self {
        Self { inner, metrics }
    }

    /// Return a `format`able structure that produces a single line
    /// per node.
    ///
    /// ```text
    /// ProjectionExec: expr=[a]
    ///   CoalesceBatchesExec: target_batch_size=4096
    ///     FilterExec: a < 5
    ///       RepartitionExec: partitioning=RoundRobinBatch(16)
    ///         CsvExec: source=...",
    /// ```
    pub fn indent(&self) -> impl fmt::Display + 'a {
        struct Wrapper<'a> {
            plan: &'a dyn ExecutionPlan,
            metrics: &'a Vec<MetricsSet>,
        }
        impl fmt::Display for Wrapper<'_> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let t = DisplayFormatType::Default;
                let mut visitor = IndentVisitor {
                    t,
                    f,
                    indent: 0,
                    metrics: self.metrics,
                    metric_index: 0,
                };
                accept(self.plan, &mut visitor)
            }
        }
        Wrapper {
            plan: self.inner,
            metrics: self.metrics,
        }
    }
}

/// Formats plans with a single line per node.
struct IndentVisitor<'a, 'b> {
    /// How to format each node
    t: DisplayFormatType,
    /// Write to this formatter
    f: &'a mut fmt::Formatter<'b>,
    /// Indent size
    indent: usize,
    /// The metrics along with the plan
    metrics: &'a Vec<MetricsSet>,
    /// The metric index
    metric_index: usize,
}

impl ExecutionPlanVisitor for IndentVisitor<'_, '_> {
    type Error = fmt::Error;
    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
    ) -> std::result::Result<bool, Self::Error> {
        write!(self.f, "{:indent$}", "", indent = self.indent * 2)?;
        plan.fmt_as(self.t, self.f)?;
        // Like `collect_plan_metrics`, an operator without metrics takes no
        // slot, so the operators after it keep their own.
        let metrics = if plan.metrics().is_some() {
            self.metric_index += 1;
            self.metrics.get(self.metric_index - 1)
        } else {
            None
        };
        if let Some(metrics) = metrics {
            let metrics = metrics
                .aggregate_by_name()
                .sorted_for_display()
                .timestamps_removed();
            write!(self.f, ", metrics=[{metrics}]")?;
        } else {
            write!(self.f, ", metrics=[]")?;
        }
        writeln!(self.f)?;
        self.indent += 1;
        Ok(true)
    }

    fn post_visit(&mut self, _plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        self.indent -= 1;
        Ok(true)
    }
}

impl ToStringifiedPlan for DisplayableBallistaExecutionPlan<'_> {
    fn to_stringified(
        &self,
        plan_type: datafusion::logical_expr::PlanType,
    ) -> StringifiedPlan {
        StringifiedPlan::new(plan_type, self.indent().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::metrics::{Count, Metric, MetricValue};
    use datafusion::physical_plan::union::UnionExec;
    use std::sync::Arc;

    fn output_rows(rows: usize) -> MetricsSet {
        let count = Count::new();
        count.add(rows);
        let mut set = MetricsSet::new();
        set.push(Arc::new(Metric::new(MetricValue::OutputRows(count), None)));
        set
    }

    /// An operator without metrics, such as an `EmptyExec`, takes no slot in
    /// the stage metrics, so the operators after it show their own.
    #[test]
    fn an_operator_without_metrics_does_not_shift_the_metrics_after_it() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let source =
            MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&schema), None)
                .unwrap();
        let union =
            UnionExec::try_new(vec![Arc::new(EmptyExec::new(schema)), source]).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(union));
        assert_eq!(collect_plan_metrics(plan.as_ref()).len(), 3);

        let formatted = format_stage_metrics(
            plan.as_ref(),
            &[output_rows(30), output_rows(20), output_rows(10)],
        );
        let lines: Vec<&str> = formatted.lines().collect();

        assert!(
            lines[0].starts_with("CoalescePartitionsExec")
                && lines[0].contains("output_rows=30"),
            "{formatted}"
        );
        assert!(
            lines[1].trim_start().starts_with("UnionExec")
                && lines[1].contains("output_rows=20"),
            "{formatted}"
        );
        assert!(
            lines[2].trim_start().starts_with("EmptyExec")
                && lines[2].ends_with("metrics=[]"),
            "{formatted}"
        );
        assert!(
            lines[3].trim_start().starts_with("DataSourceExec")
                && lines[3].contains("output_rows=10"),
            "{formatted}"
        );
    }
}
