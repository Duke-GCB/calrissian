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
the equivalent JSON path. It reuses the example policies with unique names and label values to
avoid changing other tasks' policies. It creates a temporary unprivileged task
ServiceAccount and a controlled HTTP endpoint using the deployed runner image,
then executes actual CWL tools in this order:

| Case | Expected observation |
|---|---|
| `networkAccess: true`, before denial | HTTP 200 from the control endpoint |
| `networkAccess: false` | Connection fails, tool fails, and its result records the transport error |
| `networkAccess: true`, after denial | HTTP 200 from the same endpoint |

The request uses the Service IP directly and disables HTTP proxies. DNS or
proxy failures therefore cannot stand in for the deny check. HTTP error
responses still count as connectivity, so a 403/500 response cannot pass as
isolation. The helper checks observed task labels and requires a result emitted
by the tool; image-pull, scheduling, or schema failures do not count as a passed
network-denial test. Commands have bounded timeouts.

Successful output reports `passed: true`, with connected values `true`, `false`,
`true`. Calrissian reporting `permanentFail` for the middle tool is expected.
The helper removes its policies, endpoint, ServiceAccount, labeled task pods,
and staged runner files. Result artifacts remain in the output PVC under the
printed `/output/network-check-<id>/` path. Interrupted or forcibly killed runs
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

The [recorded conformance result](comformance/v1.2.1-failures.md) demonstrates
the disabled-by-default case. It does not alone prove allowed connectivity;
use the paired controls above. The Dask profile intentionally sets
`networkAccess.enabled: false` so Gateway, scheduler, and worker communication
can work. Do not use that profile as evidence that ordinary chart network
isolation is enabled. The isolated helper installs its own scoped policies and
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
