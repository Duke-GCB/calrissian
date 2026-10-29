# Usage-report JSON

`--usage-report PATH` writes allocation-based task accounting to a runner-visible
file. It is separate from the CWL output object and CWLProv bundle. See
[collect reports](../how-to/reporting.md) for a complete invocation.

## One-task example

This illustrative report describes one task allocated two CPU cores and 512 MB
for 30 minutes, producing 10 MB of outputs. It is not a hardware measurement.

```json
{
  "cores_allowed": 4,
  "ram_mb_allowed": 1000,
  "start_time": "2026-10-27T10:00:00+00:00",
  "finish_time": "2026-10-27T10:30:00+00:00",
  "elapsed_seconds": 1800,
  "elapsed_hours": 0.5,
  "total_cpu_hours": 1,
  "total_ram_megabyte_hours": 256,
  "total_disk_megabytes": 10,
  "total_tasks": 1,
  "max_parallel_cpus": 2,
  "max_parallel_ram_megabytes": 512,
  "max_parallel_tasks": 1,
  "children": [{
    "name": "example",
    "start_time": "2026-10-27T10:00:00+00:00",
    "finish_time": "2026-10-27T10:30:00+00:00",
    "elapsed_seconds": 1800,
    "elapsed_hours": 0.5,
    "cpus": 2,
    "ram_megabytes": 512,
    "cpu_hours": 1,
    "ram_megabyte_hours": 256,
    "disk_megabytes": 10,
    "exit_code": 0,
    "node_selectors": {}
  }]
}
```

## Fields and calculations

| Scope / field | Meaning |
|---|---|
| Root `cores_allowed`, `ram_mb_allowed` | Per-run CPU/RAM budget, not observed consumption |
| Child `cpus`, `ram_megabytes` | CPU/memory requests from the recorded task completion |
| Child `start_time`, `finish_time` | Container start/finish timestamps; ISO-formatted strings |
| `elapsed_seconds`, `elapsed_hours` | Finish minus start; hours = seconds / 3600 |
| Child `cpu_hours` | Requested cores × elapsed hours |
| Child `ram_megabyte_hours` | Requested decimal MB × elapsed hours |
| Child `disk_megabytes` | Recorded output size converted from bytes to decimal MB; not scratch high-water usage |
| Child `exit_code`, `name` | Recorded task exit code and job name |
| Child `node_selectors` | Recorded placement selectors; may be empty |
| Root `start_time`, `finish_time` | Earliest recorded child start and latest recorded child finish |
| Root `total_cpu_hours`, `total_ram_megabyte_hours` | Sums across recorded children |
| Root `total_disk_megabytes` | Sum of child output sizes, not MB-hours or deduplicated storage consumption |
| Root `total_tasks` | Number of recorded children |
| Root `max_parallel_tasks`, `max_parallel_cpus`, `max_parallel_ram_megabytes` | Peak overlap calculated from recorded child intervals and their requests |

One MB here is 1,000,000 bytes. A request of `512Mi` becomes **536.870912 MB**.
CPU `500m` becomes 0.5 cores. Equal-time finish events are processed before start
events when calculating overlap. The root interval does not include all runner
startup, dependency resolution, scheduling, or image-pull time.

## Failure and schema interpretation

Report writing is attempted in the runner's finalization path. Abrupt termination
can prevent it, and failures before task completion can leave children absent.
A report with no recorded children is not evidence that a workflow succeeded;
check the runner exit status and logs. Missing timestamps may be omitted and
`elapsed_seconds` may be null. Do not treat missing records as zero consumption.

The [JSON Schema](https://github.com/Duke-GCB/calrissian/blob/develop/schemas/usage.yaml)
and [larger example](https://github.com/Duke-GCB/calrissian/blob/develop/examples/usage.json)
are useful integration inputs. The schema permits additional fields and does
not require every property. Its descriptions of resource “usage” should be
read with the implementation's allocation-based semantics above. Check actual
runtime output when building a strict downstream consumer.

CPU/RAM peaks are inferred concurrent allocations, not sampled utilization.
The report does not provide GPU utilization, peak RSS, or full Dask cluster
accounting. Use cluster telemetry for those measurements.
