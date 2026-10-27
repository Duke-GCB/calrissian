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

## Task pod and container security

Calrissian sets user/group identity at pod scope when user matching is enabled:

```yaml
spec:
  securityContext:
    runAsUser: 1001  # UID of the Calrissian runner process
    runAsGroup: 0   # GID of the Calrissian runner process
```

The actual values come from the runner process. `--no-match-user` omits these
identity overrides and allows Kubernetes/image defaults to apply.

Every generated task container and init container has its own security context:

```yaml
securityContext:
  readOnlyRootFilesystem: true
  privileged: false
  allowPrivilegeEscalation: false
```

This includes ordinary CWL tool containers, directory-preparation init
containers, and Dask main, cleanup, and cluster-initialization containers.
Directory preparation and Dask cluster initialization use distinct container
names when both are present. These settings apply to Calrissian task pods;
separately provisioned Dask Gateway scheduler/worker pods have their own backend
configuration.

`--no-read-only` sets `readOnlyRootFilesystem: false` on those containers. It
does not enable privileged execution or privilege escalation. Writable mounted
volumes remain usable with a read-only root filesystem: tools should write to
their declared output directory and temporary directory. Image filesystems are
read-only by default, so tools that need to modify them must be adapted or run
with the explicit override.

Earlier versions placed the three container settings at pod scope, where they
were not valid pod security-context fields. Correctly enforcing them can expose
workflows that previously wrote into their container image filesystem.

### Validate security scope and enforcement

After deploying the current image with `skaffold run -p dask`, run:

```sh
hatch run test.py3.10:python examples/container-security/check-runtime.py
```

The opt-in check uses the chart's deployed integration image. It strictly
validates generated ordinary and Dask manifests using server-side dry-run,
including the two-init-container Dask case. It then runs ordinary task pods with
and without the read-only override, checks their admitted security contexts,
and verifies:

- Root filesystem writes fail with `EROFS` by default (rather than just a user
  permission error), and succeed with the override.
- Processes report `NoNewPrivs: 1`.
- Output and temporary volumes are writable, including nested stdout paths
  created by the init container.

The check deletes its uniquely named runtime pods. Use
`--context <test-context>` to select another compatible test environment.
Dask integration cases are available through `task dask:test`, including case
13 for nested output and case 6 for cleanup after command failure.

## Kubernetes API credentials in task pods

Spawned ordinary and Dask workflow pods explicitly set
`automountServiceAccountToken: false` by default. This prevents Kubernetes from
automatically injecting the selected ServiceAccount's API token into task
containers, including Dask init and cleanup containers. The Pod setting takes
precedence even when the ServiceAccount enables automatic token mounting.

A workflow that intentionally calls Kubernetes can opt in at the runner:

```sh
calrissian --pod-serviceaccount workflow-api-account \
  --pod-automount-service-account-token \
  --max-ram 1G --max-cores 1 tool.cwl inputs.yaml
```

Equivalent `--conf` JSON configuration:

```json
{
  "pod_serviceaccount": "workflow-api-account",
  "pod_automount_service_account_token": true
}
```

Use JSON booleans (`true` or `false`), not strings or numbers. Nonboolean values
are rejected before task-pod submission. Omitting the setting or specifying
`false` disables automatic mounting. `--pod-serviceaccount` selects an account;
it does not opt in to token mounting. Configure the account's RBAC permissions
separately. The opt-in applies to all task pods generated by that runner and to
all containers in those pods, so use it only for workflows that need API access.

The Calrissian runner retains its own API credentials to create, monitor, and
delete task pods. This default does not change its Deployment/Job or separately
provisioned Dask Gateway components. Custom workflow code or Dask controller
scripts that previously relied on automatically mounted task credentials must
use the explicit opt-in. Standard Gateway HTTP communication does not inherently
require a mounted Kubernetes token; cluster-specific authentication may differ.

This controls automatic ServiceAccount token injection. Explicit credential
files, manually projected token volumes, or credentials supplied through other
configuration remain the deployment's responsibility.

After `skaffold run -p dask`, verify admission and runtime behavior with:

```sh
hatch run test.py3.10:python examples/task-token-automount/check-runtime.py
```

The check creates a uniquely named ServiceAccount with automatic mounting
enabled, validates ordinary and Dask manifests, and runs ordinary task pods to
check token-file absence by default and presence with opt-in. It never reads or
prints token contents and removes its pods and ServiceAccount. It also verifies
that the chart's runner still has a projected token mount. Use
`--context <test-context>` to select a compatible test cluster.
