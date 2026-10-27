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
