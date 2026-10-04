<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Ballista Metrics

Ballista exposes two kinds of metrics:

- **Scheduler metrics** in Prometheus text format, served at `GET /api/metrics`. These are optional and must be enabled at build time.
- **Executor resource metrics** (memory usage), reported to the scheduler in each heartbeat and served as JSON by `GET /api/executors`.

## Scheduler Prometheus metrics

### Enabling

Prometheus metrics are behind the scheduler's `prometheus-metrics` Cargo feature, which is **not** enabled by default.
The published binaries and Docker images are built without it. To enable it, build the scheduler yourself:

```shell
cargo build --release -p ballista-scheduler --features prometheus-metrics
```

The endpoint is part of the REST API, so the `rest-api` feature (enabled by default) must also be on, and the scheduler
must not be started with `--disable-rest-api`. When the scheduler is built without `prometheus-metrics`,
`GET /api/metrics` returns `204 No Content`.

### Available metrics

| Metric                                                  | Type      | Description                                                                                                                                      |
| ------------------------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `job_submitted_total`                                   | counter   | Jobs that finished planning and were submitted for execution.                                                                                    |
| `job_completed_total`                                   | counter   | Jobs that completed successfully.                                                                                                                |
| `job_failed_total`                                      | counter   | Jobs that failed, either during planning or during execution.                                                                                    |
| `job_cancelled_total`                                   | counter   | Jobs that were cancelled.                                                                                                                        |
| `planning_time_ms`                                      | histogram | Time in **milliseconds** from a job being queued until it is submitted for execution (planning time). Buckets: 1, 5, 25, 100, 500.               |
| `job_exec_time_seconds`                                 | histogram | Time in **seconds** from a job being queued until it completes successfully. Includes planning time. Buckets: 0.5, 1, 5, 30, 60.                 |
| `pending_task_queue_size`                               | gauge     | Intended to report tasks waiting for executor slots. Not currently updated by the scheduler, so it always reports `0`.                           |
| `ballista_scheduler_rejected_by_protocol_version_total` | counter   | Executor RPCs rejected because the executor's `BALLISTA_PROTOCOL_VERSION` did not match the scheduler's. Only appears after the first rejection. |

On Linux, the scrape also includes the standard Prometheus process metrics for the scheduler process
(`process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_open_fds`, and so on).

**NOTE** The histogram buckets are fixed. Jobs that take longer than 60 seconds, or plan for longer than 500 milliseconds,
are only counted in the `+Inf` bucket. If the defaults are not appropriate for a given use case, the only workaround is
to implement a custom `SchedulerMetricsCollector`.

### Scraping

Metrics are served on the scheduler's bind port (default `50050`), alongside gRPC and the rest of the REST API:

```shell
curl http://localhost:50050/api/metrics
```

```text
# HELP job_completed_total Counter of completed jobs
# TYPE job_completed_total counter
job_completed_total 0
# HELP job_exec_time_seconds Histogram of successful job execution time in seconds
# TYPE job_exec_time_seconds histogram
job_exec_time_seconds_bucket{le="0.5"} 0
...
```

The path is not the Prometheus default (`/metrics`), so set `metrics_path` in the scrape config:

```yaml
scrape_configs:
  - job_name: ballista-scheduler
    metrics_path: /api/metrics
    static_configs:
      - targets: ["ballista-scheduler:50050"]
```

The `ballista-cli` TUI can also display these metrics. See [CLI](cli.md).

### Custom collectors

The scheduler records metrics through the `SchedulerMetricsCollector` trait (in `ballista_scheduler::metrics`).
When embedding the scheduler, you can pass your own implementation to `SchedulerServer::new` to send metrics
to another system. `GET /api/metrics` returns whatever the collector's `gather_metrics` method produces.

## Executor resource metrics

Each executor collects memory metrics and sends them to the scheduler with its heartbeat. Select which ones with the
executor's `--metrics` (`-m`) flag:

| Value            | Collects                                                               |
| ---------------- | ---------------------------------------------------------------------- |
| `proc` (default) | Executor process memory: `proc_physical_memory`, `proc_virtual_memory` |
| `sys`            | Host memory: `total_memory`, `available_memory`, `used_memory`         |
| `all`            | Both of the above                                                      |
| `off`            | Nothing                                                                |

```shell
ballista-executor --metrics all
```

The scheduler also tracks the peak process memory seen for each executor (`peak_physical_memory`, `peak_virtual_memory`).
All values are in bytes. They are not included in the Prometheus output; read them from `GET /api/executors`
or `GET /api/executor/{executor_id}`:

```json
"metrics": [
  { "type": "proc_physical_memory", "value": 104857600 },
  { "type": "proc_virtual_memory", "value": 419430400 },
  { "type": "peak_physical_memory", "value": 125829120 },
  { "type": "peak_virtual_memory", "value": 419430400 }
]
```
