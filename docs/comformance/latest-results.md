# Latest full conformance results

All four supported CWL suites were run to completion on October 27, 2026, from
the working tree on `feature/conformance-tests`. Each runner used the same
production-derived conformance image:

```text
docker://sha256:f8013b8d063997f2fdbb986b2da7868ab05bed8c834aef64130121eb3f97906d
```

| CWL suite | Passed | Failed | Unsupported | Evidence |
|---|---:|---:|---:|---|
| 1.0.2 | 195 | 1 | 1 | [Log](../../conformance/results-v1.0.txt) |
| 1.1.0 | 245 | 2 | 6 | [Log](../../conformance/results-v1.1.txt) |
| 1.2.0 | 328 | 1 | 7 | [Log](../../conformance/results-v1.2.txt) |
| 1.2.1 | 368 | 1 | 9 | [Log](../../conformance/results-v1.2.1.txt) |

This is **1,164 suite cases: 1,136 passed, 5 failed, and 23 unsupported**.
The suites are not fully green.

## Remaining failures

- Docker ENTRYPOINT handling: test **173** in v1.0, **174** in v1.1,
  **175** in v1.2, and **177** in v1.2.1. See the
  [entrypoint diagnosis](v1.0-test-173.md).
- CWL v1.1 test **199**, `stdin_shorcut`: `wc` returns the expected numeric
  counts with different whitespace padding, failing the exact checksum and size
  comparison. See the [formatting diagnosis](v1.1-test-199.md).

In v1.2.1, all previously addressed tests (227, 229, 237, 238, 258, 333, 334,
340, 368, and 376) passed in this complete run. Nine cases were unsupported:
two require `DockerRequirement.dockerOutputDirectory` and seven require
`ToolTimeLimit`, which Calrissian now explicitly rejects. See the
[fix history](v1.2.1-failures.md).

## Validation and artifacts

- Full unit suite: **384 passed, 1 skipped**, plus 6 passing subtests on Python 3.10.
- Helm lint/render, Skaffold configuration, shell syntax, and Task dry-run checks
  passed. Task dry runs used a local substitute for the unrelated remote
  `json_utils` include.
- Refreshed full-suite logs and JSON badges are retained under `conformance/`.
  Logs have terminal color codes and trailing whitespace removed.
- Focused rerun observations are summarized in the diagnostic documents.
  Single-test logs, single-test badges, and duplicate Markdown badges are
  transient and ignored by Git. Full-suite logs provide committed evidence.
- Each suite Job finished with test-container exit code **1**, consistent with
  the recorded failures. The legacy shell runners exit after copying badges;
  their exit code alone is not a reliable suite result.

No commits were created. These results describe the tested working tree;
they do not claim that the remaining failures have been fixed.
