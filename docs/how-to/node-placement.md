# Place tasks on selected nodes

Create separate YAML **maps**, not lists, for CPU/general and CUDA task placement.
Use label keys and values that actually exist on your cluster's nodes:

```yaml
# cpu-selectors.yaml
example.org/pool: cpu
```

```yaml
# gpu-selectors.yaml
example.org/pool: gpu
```

Mount both files into the runner and add:

```sh
calrissian --max-ram 4G --max-cores 2 --max-gpus 1 \
  --pod-nodeselectors /config/cpu-selectors.yaml \
  --pod-gpu-nodeselectors /config/gpu-selectors.yaml \
  workflow.cwl inputs.yml
```

This command fragment assumes shared storage and task identity defaults have
already been configured. Use the [ConfigMap mounting pattern](labels.md) if the
files are not part of the runner's filesystem.

The current builder chooses the GPU selector map when CUDARequirement is present
in requirements or hints. Otherwise it chooses the general map. These maps are
selected alternatives, not maps merged together. A CUDA hint can therefore
select the GPU node map even when it does not result in a positive GPU allocation.

GPU tolerations use a different condition: `--pod-gpu-tolerations` is applied
only to a positive `nvidia.com/gpu` request or limit. A selector does not tolerate
a taint; combine both configurations when the selected pool is tainted. See
[GPU scheduling](scheduling.md).

Check available labels and admitted Pod placement:

```sh
kubectl get nodes --show-labels
kubectl -n YOUR_NAMESPACE get pod YOUR_TASK_POD -o yaml
kubectl -n YOUR_NAMESPACE describe pod YOUR_TASK_POD
```

Unsatisfied selectors, insufficient GPU capacity, or untolerated taints can all
leave pods Pending. These options affect task pods, not the runner's placement
or separately provisioned Dask Gateway workers.
