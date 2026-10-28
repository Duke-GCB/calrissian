## Separate runner and workflow identities

Calrissian manages Kubernetes pods, while the tools it launches execute workflow
code. Give these two roles separate ServiceAccounts. Kubernetes uses the
namespace's `default` ServiceAccount when a pod does not select one; do not bind
Calrissian's management permissions to that account.

### Create the namespace and ServiceAccounts

```sh
NAMESPACE_NAME=calrissian-demo-project
kubectl create namespace "$NAMESPACE_NAME"
kubectl --namespace="$NAMESPACE_NAME" create serviceaccount calrissian-runner
kubectl --namespace="$NAMESPACE_NAME" create serviceaccount calrissian-workflow
kubectl --namespace="$NAMESPACE_NAME" patch serviceaccount calrissian-workflow \
  --type=merge -p '{"automountServiceAccountToken":false}'
```

The workflow account has no management RoleBinding. Its token is not mounted
automatically. If a workflow needs API access, use an explicitly authorized
account with only the necessary permissions and explicitly opt in with
`--pod-automount-service-account-token` (see [task API credentials](#kubernetes-api-credentials-in-task-pods)). Selecting a ServiceAccount does not itself grant permissions.

### Grant management permissions only to the runner

```sh
kubectl --namespace="$NAMESPACE_NAME" create role pod-manager-role \
  --verb=get,create,patch,delete,list,watch --resource=pods
kubectl --namespace="$NAMESPACE_NAME" create role log-reader-role \
  --verb=get,list --resource=pods/log
kubectl --namespace="$NAMESPACE_NAME" create rolebinding pod-manager-runner-binding \
  --role=pod-manager-role --serviceaccount="${NAMESPACE_NAME}:calrissian-runner"
kubectl --namespace="$NAMESPACE_NAME" create rolebinding log-reader-runner-binding \
  --role=log-reader-role --serviceaccount="${NAMESPACE_NAME}:calrissian-runner"
```

Dask-enabled runners also create their Gateway configuration ConfigMaps:

```sh
kubectl --namespace="$NAMESPACE_NAME" create role configmap-manager-role \
  --verb=get,create,delete --resource=configmaps
kubectl --namespace="$NAMESPACE_NAME" create rolebinding configmap-manager-runner-binding \
  --role=configmap-manager-role --serviceaccount="${NAMESPACE_NAME}:calrissian-runner"
```

### Assign the identities at the correct scope

Set the runner Deployment/Job's pod template to use its management identity:

```yaml
spec:
  template:
    spec:
      serviceAccountName: calrissian-runner
      automountServiceAccountToken: true
```

Configure the Calrissian process inside that runner to select the workflow
identity for every spawned task pod:

```sh
calrissian --pod-serviceaccount calrissian-workflow \
  --max-ram 4G --max-cores 2 workflow.cwl inputs.yaml
```

Or include `"pod_serviceaccount": "calrissian-workflow"` in its JSON configuration.
`--pod-serviceaccount` configures spawned task pods, not the runner's Kubernetes
client credentials. The runner uses its own in-cluster credentials (or the
configured kubeconfig when running outside Kubernetes).

The conformance Helm chart follows the same separation: `calrissian-sa` is its
runner identity, bound to the `conformance-runner` Role; `calrissian-workflow`
is the task identity and receives no chart-created RoleBindings. Both network
profiles configure the workflow identity, and standalone suite runner Jobs
explicitly use `calrissian-sa`. The namespace's `default` account is not granted
the runner role.

### Verify permissions and migrate existing installations

Check authorization in the workload namespace with an administrator identity
that can impersonate these ServiceAccounts:

```sh
kubectl auth can-i create pods --namespace="$NAMESPACE_NAME" \
  --as="system:serviceaccount:${NAMESPACE_NAME}:calrissian-runner" \
  --as-group=system:serviceaccounts \
  --as-group="system:serviceaccounts:${NAMESPACE_NAME}" \
  --as-group=system:authenticated
# yes
kubectl auth can-i create pods --namespace="$NAMESPACE_NAME" \
  --as="system:serviceaccount:${NAMESPACE_NAME}:calrissian-workflow" \
  --as-group=system:serviceaccounts \
  --as-group="system:serviceaccounts:${NAMESPACE_NAME}" \
  --as-group=system:authenticated
# no
kubectl auth can-i get pods/log --namespace="$NAMESPACE_NAME" \
  --as="system:serviceaccount:${NAMESPACE_NAME}:calrissian-workflow" \
  --as-group=system:serviceaccounts \
  --as-group="system:serviceaccounts:${NAMESPACE_NAME}" \
  --as-group=system:authenticated
# no
```

RBAC permissions are additive. Existing namespace or cluster-wide bindings can
still grant the workflow account permissions; inspect those bindings if the
negative checks return `yes`. Separate names alone do not enforce least privilege.
The runner's ability to create pods is itself a powerful permission and should
remain limited to the runner identity.

### Migrate an existing installation

1. Create and configure dedicated runner and workflow identities.
2. Finish or stop runs that still use `default`.
3. Check whether other workloads depend on `pod-manager-default-binding` or
   `log-reader-default-binding`, then remove those old RoleBindings.
4. Review other grants to `default`.
5. Restart runners and rerun tasks to apply the new identities.

Updating documentation does not revoke existing bindings. Running pods keep
their identities.

A Helm upgrade updates the chart's runner binding and session Deployment.
Externally submitted runner Jobs also need the updated manifests.

For the local chart, run the read-only permission check after deploying it:

```sh
python3 examples/service-account-separation/check-permissions.py
```

It checks all runner management operations and denies the same operations for
both `calrissian-workflow` and `default`, including ServiceAccount group bindings.
It requires administrator impersonation privileges and does not modify RBAC.

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

GPU taints are cluster-specific, so `--pod-gpu-tolerations` has no default.

- The configured list is added only for a positive `nvidia.com/gpu` request or limit.
- CPU-only tasks, zero-GPU allocations, and CUDA hints without an actual GPU
  allocation receive no added tolerations.
- An empty list (`[]`) adds none.
- Standard `Equal`/`Exists` operators, effects, and `NoExecute` durations are supported.

Invalid configuration fails before pod submission. This includes malformed
files, unknown fields, invalid key/value syntax, and invalid combinations of
operator, effect, value, or `tolerationSeconds`.

Both ordinary and Dask task pods use this behavior. Tolerations apply to the
whole pod; the Dask cleanup sidecar still has independent CPU/memory resources
and does not request GPUs. Separately provisioned Dask Gateway schedulers and
workers require their own Gateway scheduling configuration.

A toleration permits a pod to use a tainted node; it does not reserve that node
or force placement there.
Keep the GPU resource request, appropriate node
labels/selectors, and matching taint together.
Other workloads with matching
tolerations can also use that node.

The cluster must expose GPU resources through
a suitable device plugin and provide compatible hardware, drivers, and tool
images.
This option does not validate CUDA version or hardware capability.

### Scheduler-only validation without GPU hardware

This opt-in check creates a synthetic GPU node and a temporary namespace.
It uses the actual ordinary and Dask pod builders to verify that:

- GPU pods with configured tolerations are assigned to the tainted node.
- GPU pods without tolerations remain unschedulable.
- CPU-only pods remain blocked by the taint, even with GPU configuration supplied.

```sh
hatch run test.py3.10:python examples/gpu-tolerations/check-scheduling.py \
  --context calrissian-conformance
```

Use a disposable test cluster with permission to manage nodes and namespaces.
The script removes its pods, synthetic node, and namespace in a `finally` block.
There is no kubelet or GPU device on the synthetic node: this verifies scheduler
admission only, not container startup or CUDA execution.

Real GPU execution
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

`--no-read-only` sets `readOnlyRootFilesystem: false` on those containers.
It
does not enable privileged execution or privilege escalation.
Writable mounted
volumes remain usable with a read-only root filesystem: tools should write to
their declared output directory and temporary directory.

Image filesystems are
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

Token configuration follows these rules:

- Use JSON booleans (`true` or `false`); other types are rejected before submission.
- Omitting the setting or specifying `false` disables automatic mounting.
- `--pod-serviceaccount` selects an identity; it does not enable token mounting.
- Configure the selected account's RBAC permissions separately.
- Opt-in applies to every task pod generated by that runner and all its containers.

Enable it only for workflows that need API access.

The Calrissian runner retains its own API credentials to create, monitor, and
delete task pods.
This default does not change its Deployment/Job or separately
provisioned Dask Gateway components.
Custom workflow code or Dask controller
scripts that previously relied on automatically mounted task credentials must
use the explicit opt-in.

Standard Gateway HTTP communication does not inherently
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
check token-file absence by default and presence with opt-in.
It never reads or
prints token contents and removes its pods and ServiceAccount.

It also verifies
that the chart's runner still has a projected token mount.
Use
`--context <test-context>` to select a compatible test cluster.

## Enforcing CWL network access

Calrissian selects pod labels for CWL network-access requirements. **Labels do
not restrict traffic.** The cluster operator must install matching
`NetworkPolicy` resources (or equivalent controls) and use a CNI that enforces
egress policy. Creating policies on a cluster without enforcement does not
isolate workflow pods.

The setup below uses literal `NetworkAccess` requirements:

```yaml
requirements:
  NetworkAccess:
    networkAccess: false
```

With this setup, `false` selects denied labels and `true` selects allowed labels.
When the requirement is absent, Calrissian defaults to allowed for CWL v1.0 and
denied for CWL v1.1/v1.2. The examples and validation below cover literal
requirements; they do not establish support for hints or expression-valued
network-access settings.

### Configure labels and matching policies

The repository supplies two label files:

```yaml
# examples/network-access/allowed-labels.yaml
calrissian.example/network-access: allowed
```

```yaml
# examples/network-access/denied-labels.yaml
calrissian.example/network-access: denied
```

Use the same key with different values so that the selectors do not overlap.
Install the matching policies in the namespace where **task pods** run:

```sh
kubectl --namespace="$NAMESPACE_NAME" apply \
  -f examples/network-access/network-policies.yaml
```

The denied policy selects `calrissian.example/network-access: denied` and sets:

```yaml
policyTypes: [Egress]
egress: []
```

The allowed policy selects `calrissian.example/network-access: allowed` and sets:

```yaml
policyTypes: [Egress]
egress:
- {}
```

The denied policy allows no egress covered by the cluster's policy enforcement,
including DNS. The allowed policy permits unrestricted egress; sites may instead
provide narrower destination rules. These examples do not configure ingress.

Configure Calrissian with the runtime JSON keys in
`examples/network-access/network-config.json`:

```json
{
  "network_access_pod_labels": "/etc/calrissian/network/allowed-labels.yaml",
  "no_network_access_pod_labels": "/etc/calrissian/network/denied-labels.yaml"
}
```

Alternatively, pass the equivalent CLI options:

```sh
calrissian \
  --network-access-pod-label /etc/calrissian/network/allowed-labels.yaml \
  --no-network-access-pod-label /etc/calrissian/network/denied-labels.yaml \
  --max-ram 4G --max-cores 2 workflow.cwl inputs.yaml
```

Both options populate the same plural runtime settings as the JSON configuration.
Explicit CLI values override the corresponding configuration-file defaults.

For an in-cluster runner, package the files in a ConfigMap:

```sh
kubectl --namespace="$NAMESPACE_NAME" create configmap calrissian-network-config \
  --from-file=allowed-labels.yaml=examples/network-access/allowed-labels.yaml \
  --from-file=denied-labels.yaml=examples/network-access/denied-labels.yaml \
  --from-file=network-config.json=examples/network-access/network-config.json
```

Add the corresponding volume and mount to the runner's Deployment/Job pod
specification, alongside its existing image, storage, and account configuration:

```yaml
containers:
- name: calrissian
  # Existing image, command, and other settings remain here.
  volumeMounts:
  - name: network-config
    mountPath: /etc/calrissian/network
    readOnly: true
volumes:
- name: network-config
  configMap:
    name: calrissian-network-config
```

Pass `--conf /etc/calrissian/network/network-config.json` in the runner's
Calrissian invocation, or merge the two keys into its existing configuration.
The paths refer to files visible to the runner; the tool image does not need
those files. Keep management runners separate from the labels selecting
network-denied tasks, because runners need Kubernetes API connectivity.

### Validate isolation with a reachable control

Use a disposable test cluster with policy enforcement enabled. The repository's
Minikube task selects Calico; applying the chart does not install or replace a
CNI on an existing cluster.

```sh
task conformance:cluster
skaffold run
hatch run test.py3.10:python examples/network-access/check-network.py
# Repeat using JSON configuration instead of CLI label options:
hatch run test.py3.10:python examples/network-access/check-network.py --configuration json
```

The check requires the chart's `calrissian-session` Deployment with writable
`/output` storage, and a caller authorized to execute commands in that runner
and manage test ServiceAccounts, Services, Deployments, policies, and pods.
Use `--context` and `--namespace` for another compatible test environment.

The helper uses the CLI label options by default; `--configuration json` checks
the equivalent JSON path.
It reuses the example policies with unique names and label values to
avoid changing other tasks' policies.

It creates a temporary unprivileged task
ServiceAccount and a controlled HTTP endpoint using the deployed runner image,
then executes actual CWL tools in this order:

| Case | Expected observation |
|---|---|
| `networkAccess: true`, before denial | HTTP 200 from the control endpoint |
| `networkAccess: false` | Connection fails, tool fails, and its result records the transport error |
| `networkAccess: true`, after denial | HTTP 200 from the same endpoint |

The helper avoids false positives:

- It uses the Service IP directly and disables proxies, avoiding DNS/proxy failures.
- HTTP error responses count as connectivity; a 403 or 500 does not prove isolation.
- It checks observed task labels and requires a tool-produced result.
- Image-pull, scheduling, or schema failures do not count as successful denial.
- Commands have bounded timeouts.

Successful output reports `passed: true`, with connected values `true`, `false`,
`true`.
Calrissian reporting `permanentFail` for the middle tool is expected.
The helper removes its policies, endpoint, ServiceAccount, labeled task pods,
and staged runner files.
Result artifacts remain in the output PVC under the
printed `/output/network-check-<id>/` path.

Interrupted or forcibly killed runs
may require cleanup of resources bearing that run's unique prefix/label.

This confirms TCP/HTTP enforcement to the selected endpoint on the tested
cluster/address family. It does not certify every protocol, destination, or
IPv4/IPv6 combination; extend site-specific checks where needed.

### Conformance coverage and operational limits

The chart provides the same allow/deny policy pattern through its
`networkAccess` values and supplies the label configuration to runners. With
the standard profile deployed and `networkAccess.enabled: true`, run the
existing v1.2.1 negative conformance case:

```sh
task conformance:test VERSION=1.2.1 ID=227
```

The [recorded complete conformance run](conformance/latest-results.md) demonstrates
the disabled-by-default case.
It does not alone prove allowed connectivity;
use the paired controls above.
The Dask profile intentionally sets
`networkAccess.enabled: false` so Gateway, scheduler, and worker communication
can work.
Do not use that profile as evidence that ordinary chart network
isolation is enabled.

The isolated helper installs its own scoped policies and
can also validate enforcement in that profile.

Review these deployment limits:

- NetworkPolicies are additive. Another policy selecting a denied task can grant
  egress; an empty deny rule does not override an allow rule.
- Policies act on pods, including their sidecars/init containers. Loopback and
  communication between containers in the same pod are not separate isolation
  boundaries. Denying task egress can also block controller/service connections.
- Missing or mismatched labels leave pods outside these selectors. Inspect
  admitted task labels and installed policies, not just the runner's settings.
- Policy implementation details, host networking, and node-local traffic require
  CNI-specific review. This setup is not a complete host/network sandbox.

See the [Kubernetes NetworkPolicy documentation](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
for enforcement prerequisites, additive behavior, and implementation limitations.

## Fixed container output directories

Tools can choose a fixed output path inside their container with
`DockerRequirement.dockerOutputDirectory`:

```yaml
requirements:
  DockerRequirement:
    dockerPull: example/tool:1.0
    dockerOutputDirectory: /results
```

Calrissian mounts the task's writable output directory at `/results` and uses
that path as the container working directory, `HOME`, and `runtime.outdir`.
The runner retains its own storage path for staging and output collection;
concurrent tasks can use the same container path with separate output directories.

This writable volume works with the default read-only container root filesystem.
Mounting at an existing image directory hides its original contents for that task.

Output bindings still resolve within the designated output directory.
This feature does not enable Dockerfile builds, image imports, or image loads.
