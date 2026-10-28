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
When finished, stop the development cluster with `task conformance:cluster:stop`.
To remove it and its data permanently, use `task conformance:cluster:delete`.
