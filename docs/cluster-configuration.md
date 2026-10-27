## Creating Namespace and Roles

`calrissian` executes CWL workflows by running steps as Pods in a kubernetes cluster. 

To support this requirement, we create a role with the necessary privileges and bind it to a service account.

### Create the Namespace

```
NAMESPACE_NAME=calrissian-demo-project
kubectl create namespace "$NAMESPACE_NAME"
```

### Create the Roles and RoleBindings

```
kubectl --namespace="$NAMESPACE_NAME" create role pod-manager-role \
  --verb=create,patch,delete,list,watch --resource=pods
kubectl --namespace="$NAMESPACE_NAME" create role log-reader-role \
  --verb=get,list --resource=pods/log
kubectl --namespace="$NAMESPACE_NAME" create rolebinding pod-manager-default-binding \
  --role=pod-manager-role --serviceaccount=${NAMESPACE_NAME}:default
kubectl --namespace="$NAMESPACE_NAME" create rolebinding log-reader-default-binding \
  --role=log-reader-role --serviceaccount=${NAMESPACE_NAME}:default
```
## GPU node selectors and tolerations

Calrissian can configure CPU and GPU task pods separately. Node selectors choose
eligible nodes by label; tolerations allow scheduling past matching node taints.
A selector does not override a `NoSchedule` taint.

Create `cpu-nodes.yaml` and `gpu-nodes.yaml` using labels installed on your nodes:

```yaml
# cpu-nodes.yaml
example.com/node-pool: cpu
```

```yaml
# gpu-nodes.yaml
example.com/node-pool: gpu
```

For nodes tainted with `nvidia.com/gpu:NoSchedule`, create `gpu-tolerations.yaml`:

```yaml
- key: nvidia.com/gpu
  operator: Exists
  effect: NoSchedule
```

Pass these files to the runner:

```sh
calrissian --max-ram 4G --max-cores 2 --max-gpus 1 \
  --pod-nodeselectors cpu-nodes.yaml \
  --pod-gpu-nodeselectors gpu-nodes.yaml \
  --pod-gpu-tolerations gpu-tolerations.yaml \
  workflow.cwl inputs.yaml
```

The same setting can be supplied in a `--conf` JSON file as
`"pod_gpu_tolerations": "/path/to/gpu-tolerations.yaml"`. These paths refer to
files accessible inside the Calrissian runner, not inside the tool container.

`--pod-gpu-tolerations` has no default toleration: GPU taints are cluster-specific.
When configured, Calrissian adds the list to `spec.tolerations` only when the
built task pod has a positive `nvidia.com/gpu` request or limit. CPU-only tasks,
zero-GPU allocations, and CUDA hints without an actual GPU allocation receive
no added tolerations. An empty list (`[]`) also adds none. Malformed files,
unknown fields, invalid Kubernetes key/value syntax, and invalid combinations
of operator, effect, value, or `tolerationSeconds` fail before pod submission.
Standard `Equal`/`Exists` operators, effects, and `NoExecute` durations are
supported.

Both ordinary and Dask task pods use this behavior. Tolerations apply to the
whole pod; the Dask cleanup sidecar still has independent CPU/memory resources
and does not request GPUs. Separately provisioned Dask Gateway schedulers and
workers require their own Gateway scheduling configuration.

A toleration permits a pod to use a tainted node; it does not reserve that node
or force placement there. Keep the GPU resource request, appropriate node
labels/selectors, and matching taint together. Other workloads with matching
tolerations can also use that node. The cluster must expose GPU resources through
a suitable device plugin and provide compatible hardware, drivers, and tool
images. This option does not validate CUDA version or hardware capability.

### Scheduler-only validation without GPU hardware

The following opt-in check creates a uniquely named synthetic node advertising
GPU capacity and carrying the GPU taint, plus a temporary test namespace. It
uses the actual pod builders and verifies that GPU pods with configured
tolerations are assigned to that node, while GPU pods without tolerations and
CPU-only pods with the GPU configuration remain unschedulable because of the
taint. It also checks Dask task-pod scheduling.

```sh
hatch run test.py3.10:python examples/gpu-tolerations/check-scheduling.py \
  --context calrissian-conformance
```

Use a disposable test cluster with permission to manage nodes and namespaces.
The script removes its pods, synthetic node, and namespace in a `finally` block.
There is no kubelet or GPU device on the synthetic node: this verifies scheduler
admission only, not container startup or CUDA execution. Real GPU execution
must be validated separately on a GPU-enabled cluster.
