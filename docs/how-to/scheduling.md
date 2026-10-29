# Configure resources and placement

Set runner budgets with `--max-ram`, `--max-cores`, and, for GPU workloads,
`--max-gpus`. Declare tool ResourceRequirements so tasks receive useful requests
and limits; the runner budget controls aggregate workflow concurrency.

```yaml
requirements:
  ResourceRequirement:
    coresMin: 1
    ramMin: 256
```

CWL RAM is expressed in MiB. The CLI accepts Kubernetes-style quantities such
as `4G` and `512Mi`. See the [execution explanation](../explanation/execution.md).

Use YAML maps for `--pod-nodeselectors` and `--pod-gpu-nodeselectors` to configure
CPU/GPU placement. Selectors and tolerations solve different constraints. For
GPU nodes with a NoSchedule taint, supply a YAML list:

```yaml
- key: nvidia.com/gpu
  operator: Exists
  effect: NoSchedule
```

Pass its path through `--pod-gpu-tolerations tolerations.yaml`.
It applies only
to task pods with a positive GPU request or limit; it does not give CPU-only
pods access to tainted GPU nodes.
There is no cluster-independent default.
Declare CUDARequirement and configure the NVIDIA device plugin and GPU capacity
separately.

The [GPU guide](../cluster-configuration.md) includes examples and a
scheduler simulation; that simulation does not execute CUDA on real hardware.

Use `--pod-priority-class NAME` only after an administrator creates the matching
PriorityClass. `--pod-labels labels.yaml` supplies general task labels. Cluster
capacity, affinity constraints, taints, and admission policy can still leave a
pod Pending even when the runner has available budget.

## Select an existing PriorityClass

Ask the cluster administrator for an appropriate existing PriorityClass, then
pass `--pod-priority-class NAME` in the runner invocation.
The runner sets the
spawned task Pod's `priorityClassName`; it does not create a PriorityClass or
change the runner Pod's priority.

Priority can affect cluster scheduling and
preemption, but it does not bypass Calrissian's resource budget or guarantee
immediate execution.

For complete selector-file examples and diagnostics, use
[place tasks on selected nodes](node-placement.md). For grouping and querying
tasks, use [label task pods](labels.md).
