# Cancel a run and clean up resources

Give each run a unique task label before submitting it. The
[standalone example](deployment.md) uses `calrissian.example/run: smoke-001`
through `--pod-labels`; its runner has a separate label. For another deployment,
replace the namespace, Job name, and selector in these commands.

## Preserve evidence first

Record Job status, runner logs, and task status before deleting anything:

```sh
kubectl -n calrissian-example get job smoke-001 -o yaml > smoke-001-job.yaml
kubectl -n calrissian-example logs job/smoke-001 > smoke-001-runner.log
kubectl -n calrissian-example get pods -l calrissian.example/run=smoke-001 -o yaml > smoke-001-tasks.yaml
```

Task pods are normally deleted when their termination is observed. For a
future diagnostic run, set `CALRISSIAN_DELETE_PODS=false` on the runner to retain
completed task pods. This setting does not disable all cleanup paths: shutdown
cleanup still attempts to remove tracked active pods. Use persistent
`--tool-logs-basepath` and `--stderr` destinations for retained logs.

## Stop the runner, then inspect tasks

Delete the runner Job with foreground cascading deletion and normal grace:

```sh
kubectl -n calrissian-example delete job smoke-001 --cascade=foreground --wait=true
kubectl -n calrissian-example get pods -l calrissian.example/run=smoke-001 -o wide
```

Deleting just the runner Pod can cause the Job controller to create a
replacement. Deleting the Job stops that controller from submitting another
runner. Calrissian handles SIGTERM by attempting to delete tracked task pods.
The example uses `exec calrissian` so the runner receives the signal directly.
Forced termination, node loss, missing API permissions, or exhausted grace time
can prevent cleanup.

Task pods are created directly and have no owner reference to the runner Job.
Cascading Job deletion therefore does not by itself guarantee their removal.
Review the selected pods, then remove remaining tasks for this run:

```sh
kubectl -n calrissian-example delete pods -l calrissian.example/run=smoke-001 --wait=true
kubectl -n calrissian-example get pods -l calrissian.example/run=smoke-001
```

Use the exact run selector. Keep the PVC and results reader while retrieving
outputs. Terminating a tool can leave incomplete files and cannot undo external
side effects. For Dask runs, also check Gateway for orphaned clusters using your
Gateway administration procedure; deleting task pods does not prove that remote
clusters were stopped.

## Decide whether to rerun

Watch reconnection restores monitoring; it does not recreate lost computation.
This guide does not provide a checkpoint/resume guarantee. Before resubmitting:

1. Confirm the old runner and task pods have stopped.
2. Check databases, uploaded objects, and other external side effects.
3. Archive partial outputs and logs, including any incomplete provenance.
4. Choose a fresh output/staging directory, Job name, and task-label value.
5. Rerun only after deciding whether repeating the tool's actions is acceptable.

See [execution and scheduling](../explanation/execution.md) for monitoring and
resource-accounting behavior.

## Remove the example after archiving

To remove compute resources while keeping stored results:

```sh
kubectl -n calrissian-example delete job smoke-001 --ignore-not-found
kubectl -n calrissian-example delete pods -l calrissian.example/run=smoke-001
kubectl -n calrissian-example delete pod results-reader --ignore-not-found
```

After checking the exported archive, remove the dedicated example namespace
only when its stored data is no longer needed:

```sh
kubectl delete namespace calrissian-example
```

Namespace deletion deletes its PVCs. Whether backing storage is deleted or
retained depends on the volume's reclaim policy. Do not use namespace deletion
for cleanup in a namespace shared with other workflows.
