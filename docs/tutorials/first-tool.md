# Run your first CWL v1.2 tool

You will reverse two lines in a file and retrieve the result from a task pod.
The historical tutorial used separate staging and runner Jobs; here the current
chart supplies the staging infrastructure, runner identity, and shared volumes.

## Prepare a development cluster

```sh
task conformance:cluster
skaffold run
kubectl --context calrissian-conformance -n calrissian-conformance rollout status deployment/calrissian-session
```

Skaffold builds the production Dockerfile and derives a conformance image with
cwltest. The chart creates the runner and a separate unprivileged workflow
ServiceAccount. The standard profile also installs network policies.

## Stage the tutorial files

The [downloadable fixtures](files/reverse.cwl) include two tools, one workflow,
job orders, and two text inputs. Copy the directory from this checkout:

```sh
python3 scripts/stage-docs-tutorial.py
```

`reverse.cwl` declares CWL v1.2, a Debian container, one File input, and a
File output collected through `stdout`. The generated name uses the input
basename, which prevents collisions when processing different input files.

## Execute the tool

```sh
kubectl --context calrissian-conformance -n calrissian-conformance exec deployment/calrissian-session -c calrissian -- calrissian --max-ram 1G --max-cores 1 --pod-serviceaccount calrissian-workflow --tmpdir-prefix /output/docs-tutorial/tmp/ --tmp-outdir-prefix /output/docs-tutorial/tmpout/ --outdir /output/docs-tutorial/reverse-results /output/docs-tutorial/reverse.cwl /output/docs-tutorial/single.yml
```

The output JSON describes `reversed`, including its location, size, and checksum.
Read the result:

```sh
kubectl --context calrissian-conformance -n calrissian-conformance exec deployment/calrissian-session -c calrissian -- cat /output/docs-tutorial/reverse-results/one.txt.reversed.txt
```

Expected content:

```text
arbez
elppa
```

The runner stages data on the output PVC. The tool's `/tmp` is a separate task
volume. Continue with [a parallel workflow](parallel-workflow.md).

## Check the deployment state

```sh
kubectl --context calrissian-conformance -n calrissian-conformance get deployment/calrissian-session
kubectl --context calrissian-conformance -n calrissian-conformance get pvc
kubectl --context calrissian-conformance -n calrissian-conformance get pods
```

The session Deployment should have one available replica and its data/output
claims should be Bound. Completed tool pods are normally deleted; their absence
after a successful command is expected. The session and chart helper resources
remain running so you can continue the next tutorial.

If rollout stalls, inspect Pod events. If staging fails, check the deployment,
namespace, and output PVC. For task scheduling, image pulls, permissions, and
missing output files, use the [troubleshooting table](../how-to/validation.md#diagnose-common-failures).

## Finish or pause

Keep the cluster running to continue with the parallel workflow. If stopping
here, first save the result to your host:

```sh
kubectl --context calrissian-conformance -n calrissian-conformance exec deployment/calrissian-session -c calrissian -- cat /output/docs-tutorial/reverse-results/one.txt.reversed.txt > one.txt.reversed.txt
task conformance:cluster:stop
```

Stopping preserves the development cluster and its volumes. Resume it with
`task conformance:cluster`, then check the session rollout before continuing.
For permanent removal after saving all needed artifacts, run
`task conformance:cluster:delete`; this deletes the entire development cluster
and its data. See [cancellation and cleanup](../how-to/cleanup.md) before stopping
an active workflow.
