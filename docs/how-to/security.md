# Configure task security and network access

Use the detailed [cluster configuration guide](../cluster-configuration.md) for
complete manifests and deployment checks.

1. Create a runner ServiceAccount with namespace-scoped management permissions.
2. Create a separate workflow ServiceAccount without management RoleBindings.
3. Run Calrissian with `--pod-serviceaccount YOUR_WORKFLOW_ACCOUNT`.
4. Leave automatic task token mounting disabled unless the task explicitly
   needs it. `--pod-automount-service-account-token` opts in; RBAC still controls
   the selected identity's permissions.
5. Keep the default read-only root filesystem and disabled privilege escalation.
   Use `--no-read-only` only for applications that require image filesystem writes.
   `--no-match-user` lets cluster/image identity defaults apply.

For network isolation, install a policy-enforcing CNI and matching policies, then
configure `--network-access-pod-label allowed-labels.yaml` and
`--no-network-access-pod-label denied-labels.yaml`. Labels alone do not block traffic.
The [network section](../cluster-configuration.md#enforcing-cwl-network-access)
contains configuration, paired connectivity controls, and limitations.

After deploying the integration chart, run the documented checks from the host:

```sh
hatch run test.py3.10:python examples/service-account-separation/check-permissions.py
hatch run test.py3.10:python examples/task-token-automount/check-runtime.py
hatch run test.py3.10:python examples/container-security/check-runtime.py
hatch run test.py3.10:python examples/network-access/check-network.py
```

Review prerequisites and context options in each script's `--help`. The security
checks target the integration environment; the network helper installs uniquely
scoped policies and uses a controlled endpoint. Other policies can add allowed
egress, and containers within one pod share a network namespace.

## Inspect security scopes

At pod scope, user matching sets `runAsUser` and `runAsGroup` from the runner.
At container scope, each generated regular and init container receives
`readOnlyRootFilesystem: true`, `privileged: false`, and
`allowPrivilegeEscalation: false`. The latter two are container settings, not
pod-level securityContext fields. Separately managed Dask Gateway workers are
outside these task builders.

`--no-read-only` is an explicit override, not the default. It changes filesystem
writability while leaving privileged execution and privilege escalation disabled.
Writable output and scratch mounts remain usable with the default read-only root.
