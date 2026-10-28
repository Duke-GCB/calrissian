# Collect results, reports, and provenance

Use writable persistent destinations accessible to the runner:

```sh
calrissian --max-ram 4G --max-cores 2 \
  --tmpdir-prefix /shared/tmp/ --tmp-outdir-prefix /shared/tmpout/ \
  --outdir /shared/results \
  --stdout /shared/results.json --stderr /shared/runner.log \
  --usage-report /shared/usage.json --tool-logs-basepath /shared/tool-logs \
  --provenance /shared/provenance workflow.cwl inputs.yml
```

Create staging parents first and use a fresh provenance directory per run.
Retrieve persistent artifacts after the runner finishes, even if its Pod has
been removed.

## Choose the right artifact

- `--stdout`: the runner's CWL output object. A tool's CWL `stdout` field instead
  creates a tool output file.
- `--stderr`: runner diagnostics and forwarded tool logs.
- `--tool-logs-basepath`: collected per-job container logs.
- `--usage-report`: allocation-based accounting, timing, exit status, and output
  size. It is not sampled utilization telemetry; see
  [accounting versus measurements](../explanation/execution.md#allocations-measurements-and-reporting).
- `--provenance`: a CWLProv Research Object packaged using BagIt, containing
  workflow/job records, logs, data, and provenance serializations.

See the [bundle reference](../reference/provenance.md) for provenance file paths.
The checked cwltool 3.3.20260925135507 output declares CWLProv 0.6.0 and has no
`ro-crate-metadata.json`. A provenance bundle is not automatically an RO-Crate.

## Review before archiving

- Inspect the bundle's manifest and checksums.
- Review payloads and logs for sensitive data; provenance can retain inputs.
- Check that finalization completed. Abrupt termination can interrupt it.

A successful archive does not prove every semantic provenance relationship is complete.
