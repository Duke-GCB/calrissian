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
account with only the necessary permissions and configure token mounting
accordingly. Selecting a ServiceAccount does not itself grant permissions.

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

For an installation following the old guide, create/configure the dedicated
runner and workflow identities first, then remove the old
`pod-manager-default-binding` and `log-reader-default-binding` RoleBindings after
checking that other workloads do not depend on them. Finish or stop runs still using
`default` before revoking its management bindings. Review any other grants to
`default`. Updating the guide does not revoke bindings already installed.
Existing running pods keep their identity; restart runners and rerun tasks to
apply the new configuration. A Helm upgrade updates the chart's runner binding
and session Deployment, but externally submitted runner Jobs need the updated
manifests too.

For the local chart, run the read-only permission check after deploying it:

```sh
python3 examples/service-account-separation/check-permissions.py
```

It checks all runner management operations and denies the same operations for
both `calrissian-workflow` and `default`, including ServiceAccount group bindings.
It requires administrator impersonation privileges and does not modify RBAC.
