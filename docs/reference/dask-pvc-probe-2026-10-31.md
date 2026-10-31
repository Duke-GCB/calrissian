# Dask task PVC propagation probe — 31 October 2026

The opt-in `--dask-mount-task-pvcs` feature was tested on the existing
`eurac-openeo` minikube cluster, namespace `openeo`, without committing changes.

## Test and result

`dask-gateway/tests/pvc-read.cwl` takes a normal CWL File input and submits its
read to a Gateway worker. It asserts the exact text and writes the worker
address, path and cluster identity into `result.json`.

The final run reported **success**:

| Observation | Result |
| --- | --- |
| Worker | `tls://10.244.0.70:40047` |
| Cluster | `openeo.03c57909e260472492b3806fe43ca7bc` |
| Input text | `Read this file from the execution PVC.` followed by a newline |
| Input path | `/var/lib/cwl/stg175577a9-a4ca-4aaf-94af-3691d9c2dd10/pvc-input.txt` |
| Task versus worker mounts | Both resolved PVC mounts matched on claim, subpath, container path and read-only mode. |
| Input access | Staged input mount was read-only. |
| Task credentials | Service-account token automount remained disabled. |
| Cleanup | The cluster reached `Stopped`. |

The two propagated mounts were the task output directory and the staged input
file. Pod-local coordination volumes and configuration/credential volumes were
not propagated by the feature. Existing profile-specific worker configuration
was retained.

Storage was a dedicated PVC declared `ReadWriteMany`, backed by a host-path PV
with affinity to the single minikube node. This validates multiple-pod access
and exact subpath mapping on that node; it does not validate a production,
multi-node RWX storage provider.

## Environment and instrumentation

The probe runner and workers used the locally built
`calrissian-pvc-probe:verified` image. Dask, Distributed and Dask Gateway clients
were aligned at 2026.3.0. The Gateway test image added JSON Schema validation.
The existing openEO Gateway profile was temporarily adapted to accept the
`worker_pvc_mounts` option and allow only the dedicated probe PVC.

The operator initialization hook translated this deployment's upper-case budget
options and forwarded the mount descriptor. Its disposal hook included a
20-second observation delay so pod specifications could be captured. This delay
was test instrumentation; no delay was added to the extension's default lifecycle.
The original Gateway image/configuration was restored after evidence collection,
and temporary probe runner, hook ConfigMap, PVC and PV resources were removed.

An initial attempt failed during Gateway client connection with
`CommClosedError`. Subsequent independent runs succeeded, including the final
exact-source-verified image run. No connection retry workaround was added to the feature; the initial
connection failure was not established to be a mount problem.

The final image was checked against the checkout: Python syntax trees matched
for all extension modules and schema bytes matched exactly. This allows
formatting differences without accepting functional source differences.

## Source checks and retained evidence

The full Calrissian suite passed: **554 tests**, one skipped, and 51 subtests.
Nine focused PVC tests cover opt-in behavior, filtering, read-only preservation,
wire-schema validation, claim approval, path/collision rejection and lifecycle
transport. The earlier extension type check passed with missing-import handling;
see the stricter final check below. Focused lint/format and security
checks cover the new mount module, configuration renderer and tests; plugin lint
and formatting were also checked.

Strict MkDocs validation remains failing with 17 warnings about missing generated
API/UML documentation and an existing example link. The untouched `HEAD` snapshot
produces the identical 17 warnings; this feature introduces no additional warnings.
These documentation-build prerequisites are not reported as passed.

Evidence is retained outside the documentation site, under repository-relative
`dask-gateway/verification-results/pvc-mounts-2026-10-31/`:

- `evidence.json`: filtered task/worker mount and image details, PVC access modes,
  cluster cleanup state and built wheel hashes.
- `result.json`: worker response.
- `run.log`, `task.log`: CWL and lifecycle diagnostics.
- `unit-tests.log`: full Calrissian unit-test result.

## Subsequent read/write regression

The initial probe established input reads and mount metadata, but did not perform
an output write. The Sentinel-2 integration subsequently exposed `EROFS` in the
worker output directory: multiple volume sources for the same PVC mixed
source-level read-only restrictions. The helper now reuses one volume per claim
and preserves per-path restrictions on volumeMounts. An explicitly read-only
existing profile source cannot be upgraded to writable access.

The strengthened `pvc-read.cwl` submits a worker task that reads the input and
writes an output, verifies the task can read the worker's file, and requires
`EROFS` when opening the staged input for writing. It passed with the openEO
runtime image and the generic `calrissian-pvc-probe:cohort` image. The generic image
pins msgpack 1.2.1 and Tornado 6.5.9 to the working runtime cohort. Earlier attempts
with msgpack 1.2.4/Tornado 6.6 failed with `scheduler-connection-lost`; the individual
cause was not isolated, and product dependency defaults were not changed.

Evidence for this regression is under the same directory's `read-write-fix/`
subdirectory. Cluster `openeo.58ec3062ee974f10a7682e1580e9f083` reached `Stopped`.
The updated full suite passed **555 tests**, one skipped and 51 subtests. The
strict documentation-build limitation described above remains.

The Sentinel-2 run also passed after this fix, with HDF5 locking at its default;
disabling locking alone had not fixed the failure. Its detailed record belongs
to the sibling openeo-calrissian repository. Local filesystem `load_stac` support
remains separate.

The final unsuppressed mypy check of the whole extension reports 18 errors involving
untyped Gateway imports, Kubernetes stub compatibility and container-status
annotations. The new mount module passes independently. The broader type gate is
not passed; its output and dependency setup are retained in `read-write-fix/`.
