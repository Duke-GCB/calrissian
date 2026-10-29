# Validate a deployment and investigate failures

Run one official suite case against the deployed conformance environment:

```sh
skaffold run
task conformance:test VERSION=1.2.1 ID=107
```

Supported selectors are `1.0`, `1.1`, `1.2`, and `1.2.1`; test numbers are local
to each suite. The runner prints its Job name and local log path and retains the
Job for inspection. Read the verdict and exit code rather than assuming a
successful artifact-copy command means the test passed.

For complete runs, inspect the existing version-specific manifests and runners
under [conformance](https://github.com/Duke-GCB/calrissian/tree/develop/conformance).
Use the [recorded results](../conformance/latest-results.md) as dated evidence,
not a guarantee about an arbitrary image or cluster. Focused reruns after fixes
do not update historical full-suite totals automatically.

For failures, inspect the runner log, task Pod events and termination status,
PVC mappings, selected ServiceAccount, and installed NetworkPolicies. A missing
output may be secondary to a failed command. ENTRYPOINT compatibility remains
tracked in #207; ToolTimeLimit is explicitly unsupported.

Run local regressions from a checkout:

```sh
hatch run test.py3.10:python -m pytest -q
```

Watch recovery can be checked with injected expired resource versions without
requiring a cluster (see regression-test PR #251). Such checks do not reproduce
node drains, production outages, or GPU autoscaling.

## Diagnose common failures

Start by deciding whether the **runner** failed, a **task pod** failed, or the
workflow is waiting inside Calrissian before a task is submitted. Use the
namespace and unique task selector from your run; these commands match the
[standalone example](deployment.md).

```sh
kubectl -n calrissian-example get jobs,pods,pvc -o wide
kubectl -n calrissian-example get pods -l calrissian.example/run=smoke-001
kubectl -n calrissian-example get events --sort-by=.metadata.creationTimestamp
kubectl -n calrissian-example logs job/smoke-001
```

Set `TASK_POD` to a task name returned by the selector:

```sh
export TASK_POD=replace-with-task-pod-name
kubectl -n calrissian-example describe pod "$TASK_POD"
kubectl -n calrissian-example get pod "$TASK_POD" -o yaml
kubectl -n calrissian-example logs "$TASK_POD" --all-containers=true
```

| Symptom | Evidence to inspect | Next action |
|---|---|---|
| PVC stays Pending | `kubectl -n calrissian-example describe pvc shared` and storage events | Confirm the StorageClass exists and supports the requested access mode and capacity. A WaitForFirstConsumer class may bind only after a consuming Pod is scheduled. |
| Pod Pending / FailedScheduling | Pod events and resource requests | Resolve insufficient capacity, quota, selectors, taints, or unbound claims. Increasing Calrissian's budget does not add cluster capacity. |
| No task submitted while runner remains active | Runner logs, CWL dependencies, and aggregate resource budgets | Check whether the next tool can fit within `--max-ram`, `--max-cores`, and `--max-gpus`. Distinguish this from an existing Kubernetes Pod waiting for a node. |
| ErrImagePull / ImagePullBackOff | Pod events showing the image and registry error | Correct the image tag, registry reachability, and pull credentials on the ServiceAccount used by that Pod. Runner and task accounts are separate. |
| Forbidden when creating or watching pods | Runner log, runner `serviceAccountName`, and RoleBindings | Grant the runner the documented namespace-scoped pod/log permissions. Giving management permissions to the task account does not fix runner authentication. |
| Could not find a persistent volume mounted for a path | Runner mounts, staging prefixes, and TMPDIR | Place cross-pod inputs and staging files on a discovered PVC. Make the runner the first container; runner-local `/tmp` cannot be mounted into another pod. |
| Permission denied on shared files | Pod user/group settings, file ownership, and storage-driver behavior | Make directories writable to the configured identities using the storage system's permission model. Check fsGroup support and NFS ownership rules. |
| Read-only file system inside a tool | Failing path and declared outputs | Write to the mounted output/scratch paths, or declare `dockerOutputDirectory` for a fixed output path. Use `--no-read-only` only if image filesystem writes are required. |
| OOMKilled | Pod YAML: container status, reason, exit code, and memory limits | Identify whether the runner or tool was killed. Increase that container's memory allocation; task ResourceRequirements and runner Pod limits are separate. Exit code 137 alone does not establish OOM. |
| Tool cannot reach an endpoint | DNS errors, effective NetworkPolicies, CNI enforcement, and endpoint reachability | Check DNS and allowed destinations. CWL NetworkAccess and labels need matching policies; Dask workers and scheduler have separate connectivity requirements. |
| Pod vanished or node was drained | Pod/node events and runner monitoring logs | Preserve evidence and follow the rerun procedure in [cleanup](cleanup.md). Watch recovery does not recreate lost task computation. |
| No output file after a nonzero exit | Tool logs, CWL output glob, command, and exit status | Fix the original command failure before investigating output collection. Check the documented ENTRYPOINT limitation. |

If task pods disappear before inspection, configure a new diagnostic run with
`CALRISSIAN_DELETE_PODS=false` and persistent task logs. See
[cancellation and cleanup](cleanup.md) for retention limits. Do not rerun a tool
with external side effects until you have checked what the first attempt did.
