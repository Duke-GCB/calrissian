# Conformance limitations

Use the [latest complete results](latest-results.md) for the tested image,
full-suite totals, and links to logs. This page explains remaining limitations
across versions rather than documenting isolated reruns.

## Container ENTRYPOINT

The `docker_entrypoint` case expects the image's entrypoint to interpret the
CWL command arguments. Calrissian sets the Kubernetes container command to
`/bin/sh -c` for stream redirection, overriding the image entrypoint.

- Relevant cases: v1.0 **173**, v1.1 **174**, v1.2 **175**, v1.2.1 **177**.
- The expected `cow` output is not produced.
- Compatibility work is tracked in [#207](https://github.com/Duke-GCB/calrissian/issues/207).

Passing shell-quoting tests does not establish entrypoint compatibility.

## Exact output formatting

The v1.1 `stdin_shorcut` case (**199**, upstream spelling) compares the exact
bytes produced by `wc`, including padding.

Earlier observations showed matching numeric counts but different whitespace:

- Expected: `     16     198    1111\n` (24 bytes).
- Observed: `  16  198 1111\n` (15 bytes).

A possible explanation is that the shell redirects stdin from a regular file
rather than a pipe, changing field widths. That explanation is not confirmed.
The complete-suite verdict, rather than numeric equivalence, determines the result.

## Unsupported execution limits

Required `ToolTimeLimit` is explicitly rejected. The runner does not currently
provide execution-only deadlines that exclude image pulling and scheduling.
A Kubernetes pod deadline would have different semantics.

Unsupported cases are reported separately from failed cases. Do not interpret
unsupported results as successful enforcement of a requirement.

## Scope of the evidence

- Results apply to the image and cluster configuration recorded in the summary.
- Network-access behavior requires the matching policies and enforcing CNI.
- Conformance does not establish GPU hardware, cloud storage, autoscaling, or
  node-disruption compatibility.
- No expected outputs or tests are modified to turn failures into passes.
