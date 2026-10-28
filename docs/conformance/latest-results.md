# Latest complete conformance results

All four suites completed on **2026-10-27** on the Minikube
`calrissian-conformance` cluster, using the standard Skaffold/Helm deployment
with network-access policies enabled.

## Tested image

- Source commit: `a79ce4f8bfac9141c949dff3aab9250c347c8106`.
- Branch: `feature/issue-244-documentation` with documentation changes in the working tree.
- Production-derived conformance image: `conformance:947fddbd2975f4a441a73667c8e1cca88ce788a92ea8ca2a9f602bb7acb98192`.
- Image ID: `docker://sha256:947fddbd2975f4a441a73667c8e1cca88ce788a92ea8ca2a9f602bb7acb98192`.
- The image was rebuilt with `skaffold run` before these runs.

## Complete-suite verdicts

| CWL suite | Passed | Failed | Unsupported | Exit code | Evidence |
|---|---:|---:|---:|---:|---|
| 1.0.2 | 196 | 1 | 0 | 1 | [Log](https://github.com/Duke-GCB/calrissian/blob/develop/conformance/results-v1.0.txt) |
| 1.1.0 | 246 | 2 | 5 | 1 | [Log](https://github.com/Duke-GCB/calrissian/blob/develop/conformance/results-v1.1.txt) |
| 1.2.0 | 330 | 1 | 5 | 1 | [Log](https://github.com/Duke-GCB/calrissian/blob/develop/conformance/results-v1.2.txt) |
| 1.2.1 | 370 | 1 | 7 | 1 | [Log](https://github.com/Duke-GCB/calrissian/blob/develop/conformance/results-v1.2.1.txt) |

Across the suites: **1,164 cases**, **1,142 passed**,
**5 failed**, and **17 unsupported**. Suites overlap in behavior;
these totals count suite cases rather than unique features.

The suites are not fully green. See [limitations](limitations.md) for the
remaining failures and unsupported behavior.

## Artifacts and interpretation

- Full logs are retained under `conformance/results-v*.txt` in the repository.
- Category badge JSON files under `conformance/badges/` were refreshed from each run.
- [Machine-readable run metadata](https://github.com/Duke-GCB/calrissian/blob/develop/conformance/full-results.json)
  records image, source, Job names, counts, and container exit codes.
- No tests or expected outputs were changed for this run.
- A nonzero suite-container exit code is expected when cases fail or report
  unsupported features. Successful log/artifact collection is not a suite pass.

These complete runs supersede the earlier focused-rerun documentation. Counts
and category tables describe this image and cluster configuration, not arbitrary
cloud storage, GPU hardware, network policies, or infrastructure disruption.
