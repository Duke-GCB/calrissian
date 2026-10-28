# Runner environment variables

These variables configure the runner process, not arbitrary task environments.
Use the downward API to populate `CALRISSIAN_POD_NAME` from `metadata.name`.

| Variable | Default / meaning |
|---|---|
| `CALRISSIAN_POD_NAME` | Required runner Pod name for PVC discovery |
| `CALRISSIAN_DELETE_PODS` | `true`; `false`, `no`, or `0` retains completed task pods |
| `RETRY_MULTIPLIER` | `5`, exponential retry multiplier |
| `RETRY_MIN` | `5`, minimum retry wait in seconds |
| `RETRY_MAX` | `1200`, maximum retry wait in seconds |
| `RETRY_ATTEMPTS` | `10`, maximum configured attempts |
| `CALRISSIAN_DASK_CLEANUP_CPU_REQUEST` | `10m` |
| `CALRISSIAN_DASK_CLEANUP_MEMORY_REQUEST` | `64Mi` |
| `CALRISSIAN_DASK_CLEANUP_CPU_LIMIT` | `100m` |
| `CALRISSIAN_DASK_CLEANUP_MEMORY_LIMIT` | `256Mi` |

Dask cleanup quantities must be positive and requests must not exceed limits;
CPU precision is one millicore. They do not alter worker-cluster budgets.
Retry settings do not make all failures retryable: the current policy excludes
ordinary HTTP 4xx errors; watch expiration (410) is handled explicitly by monitors.
Use [task configuration](../how-to/configuration.md) for imported task environment.
