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
