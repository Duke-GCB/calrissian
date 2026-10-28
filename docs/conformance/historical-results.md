# Historical conformance logs

The following files are retained for historical investigation. They are
byte-identical captured outputs beneath their archival headers:

| Retained URL | Status |
|---|---|
| [Original CWL v1.0 log](../result-v1-0.txt) | Historical capture, not current conformance evidence |
| [Duplicate CWL v1.0 log](result-v1-0.txt) | Same capture retained at its other existing URL |

Git history records the original log in November 2023. That is a repository
history date, not independently verified execution metadata. The capture lacks
the source/image and cluster metadata used by the current result summaries,
and ends with a `FileExistsError` while creating the badge directory. It must
not be interpreted as a successful artifact-generation run or a current suite
verdict.

For current dated evidence, use [latest complete results](latest-results.md),
the version-specific result pages, and [limitations](limitations.md). The current
raw logs live under `conformance/results-v*.txt` in the repository; do not combine
their counts with these historical captures.
