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

Validation after merging the latest `develop`: **406 unit tests passed,
1 skipped**, with 51 passing subtests. Default and Dask-profile Helm rendering/lint and Skaffold
configuration checks also passed. After merging `develop`, the rebuilt Skaffold
profile and direct Python smoke test passed again, with cleanup peak RSS of
59,520 KiB (58.1 MiB) and the Gateway cluster confirmed stopped.

To remove the test environment:

```sh
skaffold delete -p dask
```

Helm hook Jobs and chart-installed CRDs may remain after uninstall. Deleting the
dedicated Minikube profile with `task conformance:cluster:delete` removes the
whole local cluster and its data.

## CWL extension compatibility suite

Deploy the environment above, then run all cases or select one stable ID:

```sh
skaffold run -p dask
task dask:test
task dask:test ID=9
# Equivalent commands without Task:
python3 dask-gateway/run-tests.py
python3 dask-gateway/run-tests.py --id 9 --timeout 600
```

The suite uses CWL v1.2 fixtures and a standard-library Python controller. It
runs Calrissian directly rather than requiring `cwltest` in the worker image.
These are extension compatibility results, separate from official CWL
conformance results. Fixture definitions and stable IDs live in `tests/`.

| ID | Case | Expected behavior |
|---|---|---|
| 1 | Minimal Dask tool | One registered worker computes 6 |
| 2 | CPU budget | CPU budget limits the cluster to two workers |
| 3 | Memory budget | Memory budget limits the cluster to two workers |
| 4 | Combined budgets | Both budgets permit two workers |
| 5 | Ordinary tool | Computes 6 without a Dask sidecar or cluster |
| 6 | Command failure | Exit 7 becomes CWL failure; cluster still shuts down |
| 7 | Workflow | A Dask step returns its computed output |
| 8 | Sequential workflow | Two dependent steps create and stop separate clusters |
| 9 | Resource expressions | Input references resolve all five resource fields |
| 10 | Missing resource field | CWL validation rejects the document |
| 11 | Invalid resource type | CWL validation rejects the document |
| 12 | Workflow failure | A failed Dask step fails its workflow and cleans up |
| 13 | Nested output | Directory preparation and Dask initialization run with container security settings |

Every case has independent `cwl` and `lifecycle` verdicts. Successful tools must
produce the expected output, and Dask workloads wait for the expected worker
count and execute a distributed computation. The controller observes task pods,
checks the sidecar's default resource specification, reads exact cluster names
from init/cleanup logs, and waits until owned clusters stop and their scheduler
and worker pods disappear. It checks the number of distinct clusters against
the case expectation. Expected failures also require a matching CWL diagnostic;
a failed Kubernetes Job alone is insufficient.

Each case creates a uniquely named runner Job and ConfigMap. The ConfigMap
combines the chart's installed controller wrappers with the current local CWL
fixtures; editing suite fixtures does not require rebuilding the worker image.
The runner and task default image use the immutable image from the chart's
session Deployment. Each Job has a deadline (600 seconds by default), no
retries, and its own output directory. The host has a matching deadline and
bounded Kubernetes requests. SIGINT and SIGTERM trigger cleanup and stop the
suite. Cleanup targets only the case's Job, labeled task pods, ConfigMap, and
cluster names recorded by that case. A forcibly killed controller cannot run
cleanup; inspect saved identities before manually removing residual resources.

Host artifacts are written under the ignored directory
`dask-gateway/test-results/suite-<run-id>/`: `results.json` records the deployed
image and both verdicts, `junit.xml` is suitable for CI, and runner/tool logs
explain individual failures. Output files remain on the PVC under
`/output/dask-test-<id>-<run-id>/`. The controller exits nonzero if any case or
cleanup fails and continues through ordinary failures to collect other results.
It expects default cleanup resources, as does the existing cleanup smoke test.

Coverage still to add: nonpositive/insufficient budgets, concurrency, external
cluster failures, resource overrides in real pods, and optional GPU hardware.
In particular, invalid budgets must be rejected before cluster creation before
adding those real-cluster cases: the current init script validates some limits
after provisioning. Expression coverage currently uses input references;
JavaScript expression blocks need separate cases. Unit tests cover expression
evaluation without mutation of a shared workflow requirement, status ordering,
and independence of computation and lifecycle verdicts.

Validated on October 27, 2026 with `skaffold run -p dask` followed by
`task dask:test`: **12/12 cases passed both verdicts**, including shutdown and
pod removal for all **10** Dask clusters created by the suite. The single-case
entry point `task dask:test ID=5` also passed. The final resource audit found no
active Gateway clusters, suite runner Jobs, or suite ConfigMaps. Unit validation:
**413 passed, 1 skipped**, with 51 passing subtests. A deliberate Ctrl-C run also
confirmed ownership recovery and shutdown of the interrupted case's cluster.
JSON, JUnit, and raw logs for
the successful full run are in the local ignored directory
`test-results/suite-91a54e3e/`.
