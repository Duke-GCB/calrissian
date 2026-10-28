# Run a parallel workflow

Complete [the first tool tutorial](first-tool.md) first; it stages all files used
here. This example retains the historical reverse-and-sort workflow structure
but uses CWL v1.2 throughout.

Inspect [workflow.cwl](files/workflow.cwl). Its `reverse` step scatters over the
input File array; its `sort` step scatters over the resulting files. Each tool
requests one core and 64 MiB of RAM. Calrissian can execute independent tasks
concurrently within the two-core and 1 GiB runner budget.

```sh
kubectl --context calrissian-conformance -n calrissian-conformance exec deployment/calrissian-session -c calrissian -- calrissian --max-ram 1G --max-cores 2 --pod-serviceaccount calrissian-workflow --tmpdir-prefix /output/docs-tutorial/tmp/ --tmp-outdir-prefix /output/docs-tutorial/tmpout/ --outdir /output/docs-tutorial/workflow-results --usage-report /output/docs-tutorial/usage.json /output/docs-tutorial/workflow.cwl /output/docs-tutorial/array.yml
```

The returned `sorted` array contains two Files. Array position follows the input
order even if tasks finish in another order. For the first file, expected lines
are `arbez` followed by `elppa`; the second contains `ananab` followed by `raep`.

```sh
kubectl --context calrissian-conformance -n calrissian-conformance exec deployment/calrissian-session -c calrissian -- cat /output/docs-tutorial/workflow-results/two.txt.reversed.txt.sorted.txt
```

Retrieve the report directly from the runner:

```sh
kubectl --context calrissian-conformance -n calrissian-conformance exec deployment/calrissian-session -c calrissian -- cat /output/docs-tutorial/usage.json
```

The [reporting guide](../how-to/reporting.md) adds provenance generation.
The [scheduling explanation](../explanation/execution.md) describes the budgets.

## Check and save the results

The workflow should return two output Files. Its four command-line tasks
(two reverse and two sort tasks) should appear in the usage report with zero
exit codes. The two-core budget allows at most two of these one-core tasks at a
time; the report's overlap may be lower for a short run. Completed task pods
are normally deleted, while the session Deployment remains available.

Save all tutorial inputs, outputs, and the usage report to your host:

```sh
kubectl --context calrissian-conformance -n calrissian-conformance exec deployment/calrissian-session -c calrissian -- python -c \
  'import sys, tarfile; t = tarfile.open(fileobj=sys.stdout.buffer, mode="w|"); t.add("/output/docs-tutorial", arcname="docs-tutorial"); t.close()' \
  > docs-tutorial.tar
tar -tf docs-tutorial.tar
```

If the workflow fails, inspect its runner output before output-collection
errors. Use [troubleshooting](../how-to/validation.md#diagnose-common-failures)
for scheduling, volume discovery, and image failures, and the
[usage-report reference](../reference/usage-report.md) to interpret the report.

## Clean up the development environment

After the workflow finishes and you have checked the archive, pause the cluster:

```sh
task conformance:cluster:stop
```

This preserves its volumes. Restart with `task conformance:cluster` if you want
to continue. To remove the entire cluster and its data permanently after saving
everything you need:

```sh
task conformance:cluster:delete
```

These operations affect every workload on the dedicated Minikube profile.
For an active run or a namespace shared with other workflows, follow
[cancellation and cleanup](../how-to/cleanup.md) instead.
