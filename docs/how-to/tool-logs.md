# Save and inspect tool logs

Set `--tool-logs-basepath` to a persistent directory writable by the runner:

```sh
calrissian --max-ram 4G --max-cores 2 \
  --tool-logs-basepath /shared/tool-logs \
  --stdout /shared/results.json --stderr /shared/runner.log \
  workflow.cwl inputs.yml
```

This fragment assumes the runner's staging, output paths, and identity are
already configured. The runner creates the log directory if needed and writes
`<job-name>.log` files during task finalization. Entries contain the collected
timestamp, pod name, and log text.

These are the logs captured from the task's Kubernetes container-log stream.
They are not a recursive collection of every log file an application creates.
If CWL redirects a tool's stdout or stderr into files, those bytes belong to
those files and need not appear in the container-log stream.

Declare files you
need as CWL outputs and retrieve them through the output object.

`--stderr` captures runner diagnostics, including forwarded tool logs;
`--stdout` captures the CWL output object. Neither replaces the tool's own CWL
stdout/stderr declarations. See [reporting](reporting.md) for usage and provenance
artifacts, which have separate formats and lifecycles.

For immediate diagnosis, inspect task events and current pod logs before
cleanup. Log collection depends on access to the pod-log API and successful
streaming; missing logs are not proof that a tool did not run. Retaining task
pods can help investigations but requires subsequent cleanup.
