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
task dask:deploy
task dask:cleanup:test
```

The `dask` profile builds the current production Dockerfile and a derived
`worker` image containing the selected Dask and Distributed release, plus the
Dask Gateway client configured in `versions.json`. That file also selects the
Helm chart version and the Dask/Distributed releases. The profile installs the
configured chart in namespace
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
task dask:deploy
task dask:test
task dask:test ID=9
# Run each version in dask-gateway/versions.json sequentially
task dask:test:matrix
# Run only case 9 against each configured version
task dask:test:matrix ID=9
# Select a smaller version list
task dask:test:matrix VERSIONS=2025.3.1
# Equivalent commands without Task:
python3 dask-gateway/run-tests.py
python3 dask-gateway/run-tests.py --id 9 --timeout 600
```

The suite uses CWL v1.2 fixtures and a standard-library Python controller. It
runs Calrissian directly rather than requiring `cwltest` in the worker image.
These are extension compatibility results, separate from official CWL
conformance results. Fixture definitions and stable IDs live in `tests/`.

The version matrix is in `versions.json`. `task dask:deploy` reads the Gateway
client/chart versions from that file. Choose a Dask release with
`task dask:deploy DASK_VERSION=2026.8.0`. The suite checks the deployed Dask,
Distributed, Gateway client, and Helm chart versions against the file and fails
before running cases if any value differs. The matrix task rebuilds and deploys
each tuple sequentially and runs the complete suite. It removes Gateway between
versions while retaining the conformance release and its PVCs, avoiding a race
between PVC deletion and the next staging Job. It removes both releases and
their PVCs when the matrix finishes. It uses the dedicated local conformance
cluster.

The real-cluster matrix passed on October 30, 2026: **13/13 cases** passed for
Dask/Distributed 2024.1.0, 2025.3.1, and 2026.8.0, using Gateway client and
Helm chart 2026.3.0. Each release leg verified its deployed versions and
completed cleanup. The matrix results and per-release logs are in
`test-results/matrix-86a79184/`.

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

### What a passing case means

Every case reports independent `cwl` and `lifecycle` verdicts.

- Successful tools produce the expected output.
- Dask tools wait for the expected worker count and execute a distributed computation.
- Task pods have the expected cleanup-sidecar resources.
- Recorded cluster names match the expected number of distinct clusters.
- Owned clusters stop, and their scheduler/worker pods disappear.
- Expected failures include a matching CWL diagnostic. A failed Kubernetes Job
  alone does not count as a passing negative test.

### Resources created by the controller

Each case uses a unique runner Job, ConfigMap, and output directory.

- The ConfigMap combines installed controller wrappers with local CWL fixtures.
  Fixture edits do not require rebuilding the worker image.
- Runner and default task images use the session Deployment's immutable image.
- Jobs have no retries and a 600-second deadline by default.
- Host commands also have deadlines and bounded Kubernetes requests.

### Interruption and cleanup

SIGINT and SIGTERM stop the suite and trigger cleanup of resources owned by the case:

- Its runner Job and ConfigMap.
- Its labeled task pods.
- Cluster names recovered from its initialization and cleanup records.

A forcibly killed controller cannot clean up. Use the saved identities to inspect
residual resources before removing them manually.

### Results and artifacts

Host artifacts are stored under the ignored directory
`dask-gateway/test-results/suite-<run-id>/`:

- `results.json`: deployed image and both verdicts.
- `junit.xml`: CI-compatible results.
- Runner/tool logs: failure details.

Output files remain on the PVC under `/output/dask-test-<id>-<run-id>/`.
The controller continues through ordinary failures to collect results and exits
nonzero if any case or cleanup fails.

The suite and cleanup smoke test expect the default cleanup resources.

### Remaining coverage

Additional cases are needed for:

- Nonpositive or insufficient resource budgets.
- Concurrent workloads and external cluster failures.
- Cleanup-resource overrides in real pods.
- Optional GPU hardware.
- JavaScript expression blocks; current expression cases use input references.

Before adding real-cluster invalid-budget cases, reject those budgets before
cluster creation. The current initialization script validates some limits after
provisioning.

Unit tests cover expression evaluation without shared-requirement mutation,
status ordering, and separate computation/lifecycle verdicts.

### Historical validation

On October 27, 2026, a full run using `skaffold run -p dask` and
`task dask:test` recorded:

- **12/12 cases** passing both verdicts.
- Shutdown and pod removal for all **10** Dask clusters created.
- A passing single-case run with `task dask:test ID=5`.
- No active Gateway clusters, suite runner Jobs, or suite ConfigMaps in the final audit.
- **413 unit tests passed**, 1 skipped, plus 51 passing subtests.

A deliberate Ctrl-C run also confirmed cluster-identity recovery and shutdown
for the interrupted case. Local ignored artifacts are in
`test-results/suite-91a54e3e/`.

These are historical results for the original 12-case run. The table now includes
case 13; the historical totals do not describe a new run of all current cases.
