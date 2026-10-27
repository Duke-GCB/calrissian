# Dask cleanup sidecar resources

The cleanup container has its own CPU/memory specification. Calrissian reads
these environment variables from the **runner process** when constructing each
task pod; setting them only inside the cleanup container does not change its
Kubernetes allocation.

| Runner variable | Default |
|---|---|
| `CALRISSIAN_DASK_CLEANUP_CPU_REQUEST` | `10m` |
| `CALRISSIAN_DASK_CLEANUP_MEMORY_REQUEST` | `64Mi` |
| `CALRISSIAN_DASK_CLEANUP_CPU_LIMIT` | `100m` |
| `CALRISSIAN_DASK_CLEANUP_MEMORY_LIMIT` | `256Mi` |

Values use Kubernetes quantity syntax. CPU must be a multiple of one millicore.
Empty, invalid, non-finite, non-positive quantities and requests exceeding their
limits cause pod construction to fail with the offending variable name. These
settings never inherit main-container resources or GPU requests and do not
change Dask worker budgets.

For the chart-managed interactive runner, values can be supplied through Helm:

```yaml
runnerEnv:
  CALRISSIAN_DASK_CLEANUP_CPU_REQUEST: "20m"
  CALRISSIAN_DASK_CLEANUP_MEMORY_LIMIT: "384Mi"
```

## Reproduce the real cleanup test

Prerequisites: Docker, Minikube, Task, Skaffold v2, Helm, kubectl, and Python 3.
The chart and base Skaffold configuration are reused from
`feature/conformance-tests`; unrelated runtime fixes from that branch are not
required for this test.

```sh
task conformance:cluster
skaffold run -p dask
task dask:cleanup:test
```

The `dask` profile builds the current production Dockerfile and a derived
`worker` image containing matching Dask, Distributed, and Gateway clients
(version 2024.1.0). It installs the requested
`https://helm.dask.org/dask-gateway-2024.1.0.tgz` chart in namespace
`dask-gateway`, with Traefik exposed as ClusterIP, and the conformance chart in
`calrissian-conformance`. Gateway's backend image name/tag are populated from
Skaffold's `worker` artifact. This is a local integration setup using simple
authentication, not a production Gateway configuration.

The test Job uses the immutable image tag from the session Deployment so task
init containers can use Minikube's local image without a registry pull. It:

1. Runs the installed Calrissian Dask extension on a CWL tool with one worker.
2. Observes and verifies the cleanup container's default requests and limits.
3. Computes `sum([1, 2, 3])` through Distributed and checks the result is 6.
4. Executes the installed cleanup script through a measurement wrapper.
5. Checks its shutdown message and confirms the observed Gateway cluster is
   stopped before reporting success.

The profile disables the conformance network-isolation policies so task pods
can reach Gateway, scheduler, and worker services. Controller wrappers and the
CWL fixture are stored in the chart's `files/dask-smoke` directory. The init
wrapper saves failures to `init-error.txt` in the shared output volume; the
cleanup wrapper records Linux `ru_maxrss`, including imports and shutdown.
These wrappers exercise the installed scripts rather than replacing their
cluster-management behavior.

Results and logs remain on the output PVC under `/output/dask-cleanup-smoke`.
The test replaces only its own Job and pods labeled `app=dask-cleanup-smoke`.
It expects the default cleanup settings; override behavior is covered by unit
tests. Reapply the Skaffold profile after changing chart fixtures.

On October 27, 2026, the real computation and Gateway shutdown passed with
`10m` / `64Mi` requests and `100m` / `256Mi` limits. The final `skaffold run -p dask` followed by `task dask:cleanup:test` run
reported cleanup peak RSS of **59,300 KiB (57.9 MiB)**; an earlier successful
run measured 59,676 KiB (58.3 MiB). This is process RSS for
this image and workload, not total cgroup memory usage or a universal sizing
guarantee. The test verified the actual sidecar Pod resource specification,
not just the configured environment. The 256 MiB limit successfully accommodated initialization and
shutdown; retain configurable limits for larger Python environments.

Validation: **368 unit tests passed, 1 skipped**, including 41 Dask tests and
9 passing subtests. Default and Dask-profile Helm rendering/lint and Skaffold
configuration checks also passed.

To remove the test environment:

```sh
skaffold delete -p dask
```

Helm hook Jobs and chart-installed CRDs may remain after uninstall. Deleting the
dedicated Minikube profile with `task conformance:cluster:delete` removes the
whole local cluster and its data.
