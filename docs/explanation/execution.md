# Execution and scheduling

cwltool evaluates CWL dependencies and expressions.
Calrissian's executor queues
ready work and reserves CPU, RAM, and GPU resources before submitting a task.
Reservations prevent concurrent submissions from exceeding the workflow budget,
including while a pod is waiting for a node.

Kubernetes separately decides which
node can run each Pod based on requests, selectors, taints, and cluster capacity.

Tools execute as commands in task containers. Calrissian currently wraps those
commands in `/bin/sh -c` for stream redirection. This supports redirection but
overrides image ENTRYPOINT, causing the known #207 conformance gap. It should
not be described as full Docker command-line parity.

The runner monitors Pod status and follows logs separately. Expired resource
versions (HTTP 410) trigger re-listing and watch restart; watch timeouts also
restart monitoring. A Future completion callback restores reserved resources,
including after an exception. Injected API/watch responses can test these transitions without a cluster.

Monitoring recovery is not execution recovery. If a task pod disappears during
node disruption, reconnecting a watch does not recreate it. #96 tracks resilience
concerns. Neither ordinary conformance runs nor mocked recovery tests establish
survival of real autoscaler/drain events.

Dask adds cluster initialization and cleanup around a main task. Its main exit
code determines the tool outcome; successful cleanup must not hide failure.
Gateway scheduler/worker resource budgets are separate from task-side cleanup
resources. See the [Dask guide](../how-to/dask.md).

## Allocations, measurements, and reporting

The executor accounts for selected resource reservations; it does not poll live
CPU or memory consumption to decide whether another task can start. Work that
cannot fit remains in Calrissian's queue. A submitted Pod can independently be
Pending in Kubernetes while waiting for node capacity or placement constraints.
These are different waiting states.

`--max-gpus` limits allocated GPU devices, not GPU cores. CUDA allocation and
node eligibility still depend on the device plugin, installed drivers, and
cluster configuration. The runner's budgets are per Calrissian invocation,
not cluster-wide quotas shared across independent runners.

Usage reports combine allocated CPU/memory from task requests, container start
and finish timestamps, exit status, output size, and derived allocation-time
metrics.
CPU-hours and RAM-megabyte-hours are allocation-based estimates, not
sampled utilization or peak resident memory.
The current report model does not
provide GPU-utilization telemetry.

Use a cluster monitoring system for actual
CPU, RAM, and GPU utilization measurements.
