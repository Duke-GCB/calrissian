# CWL candidate readiness — 31 October 2026

This record covers the owner-selected Calrissian core/plugin candidate
`430ce62176b86dc61ecf5288027c3ab15d715ebe`, with the approved fixture changes
that explicitly declare `NetworkAccess: true` for Gateway tools. It is local
validation evidence; authentication and production operational policies remain
open.

## Candidate and environment

| Component | Observed version |
| --- | --- |
| Calrissian / extension | 0.19.1 / 0.1.0, same source revision |
| cwltool | 3.3.20260925135507 |
| Kubernetes Python client | 36.0.3 |
| Kubernetes server | v1.37.0 |
| Dask / Distributed / Gateway client | All 2026.3.0 |
| Gateway chart/server | 2026.3.0 |
| Calico | v3.32.2 |

The separate minikube profile `calrissian-cwl-readiness` used four CPUs and
6,144 MiB of memory. The existing `eurac-openeo` deployment was not changed.
The candidate image's core/plugin Python syntax trees matched the approved
checkout. Adding cwltest preserved installed runtime dependency constraints.
Host/node image identity and actual task, scheduler and worker image IDs were
retained; version labels alone were not used as artifact proof.

## Required-feature conformance and lifecycle results

The official suites were staged unchanged. Selection used `--tags required`;
these are not full optional-feature runs. The Calrissian runner budget was
four cores and 4G RAM, using the chart's network policies and Calico enforcement.

| Official suite | Required cases | Failures / errors / skipped |
| --- | ---: | --- |
| CWL 1.0.2 | 50 | 0 / 0 / 0 |
| CWL 1.1.0 | 65 | 0 / 0 / 0 |
| CWL 1.2.0 | 65 | 0 / 0 / 0 |
| CWL 1.2.1 | 83 | 0 / 0 / 0 |
| Total | 263 | 0 / 0 / 0 |

Counts were checked against all four JUnit reports and container exit codes.
All 13 standard Dask cases passed their CWL and lifecycle assertions, including
normal execution, expected command/workflow failure, sequential clusters,
resource expressions, and rejection of invalid extension definitions.

A separate pre-provisioning initialization fault exited with status 1, reported
its deliberate diagnostic marker, created no cluster, and did not hit the test
Job deadline. This does not cover failure after provisioning has begun.

The earlier full-suite [limitations](../conformance/limitations.md) remain
applicable disclosures, not newly verified optional-feature verdicts. The
[previous complete runs](../conformance/latest-results.md) used a different image.

## Verified security controls

- All 33 effective RBAC checks passed: runner management permissions were present;
  workflow/default identities lacked those permissions.
- Task token automount was absent by default and present when explicitly enabled.
- Ordinary and Dask pod manifests passed strict API schema validation. Real
  containers confirmed the read-only root default, explicit writable-root override,
  writable output/scratch mounts, and `NoNewPrivs`. Strict API validation is not
  evidence of a restricted Pod Security Admission policy.
- Both CLI and JSON configuration paths passed allowed/denied/allowed HTTP traffic
  controls on Calico. Denial timed out while both allowed controls returned 200.

These checks do not establish worker admission hardening, an image allowlist,
server-enforced resource caps, storage quota enforcement or authentication.

## Blocked additional probes

The shared-PVC concurrency fixture initially used `1GiB`. Both the generic
Gateway parser and built-in lifecycle accept `1G` for exactly the same 2^30
bytes, so the fixture was corrected without changing its intended allocation.

The subsequent PVC and cancellation fixtures failed during their distributed
preflight with `scheduler-connection-lost`. Cancellation was never injected.
A focused run retained the cluster for 45 extra seconds solely to collect
scheduler/worker logs. The worker registered, the scheduler remained running,
and client connections closed/reconnected before the assertion failed. The
later scheduler shutdown followed normal disposal. The logs do not establish
the initial disconnection's root cause; startup ordering is an investigation
lead, not a confirmed diagnosis.

Consequently, this run does not prove PVC concurrency, cancellation recovery,
node-loss recovery or interrupted-disposal recovery. Earlier single-node PVC
read/write evidence remains in the [filesystem probe record](dask-pvc-probe-2026-10-31.md).
All 21 clusters observed in this test profile reached `Stopped`.

## Approved validation policy and connection controls

The owner approved `dask-gateway/readiness-values.yaml` as an opt-in validation
policy. Production defaults are unchanged. It permits only the two source-verified
worker images, caps each worker at one CPU/1 GiB and the worker pool at two
workers, two CPU and 2 GiB, and enables a 60-second idle timeout.

Gateway counts scheduler overhead in its aggregate cluster limits. The profile
therefore includes the fixed scheduler limit of one CPU/512 MiB, for an aggregate
three CPU/2.5 GiB cap while preserving the approved worker budget. The initial
attempt omitted that overhead and was capped at one worker; that failed attempt
was retained rather than changing the test's expected two-worker result.

The server rejected all seven negative requests: unapproved image, excessive
requested CPU, excessive CPU limit, excessive worker memory, zero memory, and
caller attempts to override idle timeout or maximum workers. A request for three
workers was clamped to two with a server warning; two workers were observed.
An idle cluster reached Stopped without manual cleanup. The configured timeout
was 60 seconds; the measured 54–56 seconds started after the connection had
finished starting the scheduler, not at cluster creation.

An API rollout later produced a proxy 504 while the direct API remained healthy.
The copied intermediate record from that failed verification was excluded. The
isolated proxy was restarted before regenerating final policy evidence. This
rollout observation remains relevant to operations; it is not the established
cause of the earlier scheduler disconnections.

Additional startup controls used fresh processes with unchanged Dask versions,
TLS credentials and no task retries:

| Client path | Succeeded | Failed |
| --- | ---: | ---: |
| Gateway get_client before worker registration | 1 | 2 |
| Gateway get_client after observed worker registration | 2 | 1 |
| Explicit Distributed Client using the same Gateway proxy and loop | 2 | 1 |
| Direct scheduler TLS using Gateway-issued credentials and the same loop | 1 | 2 |

These small diagnostic samples do not establish a safe workaround. Neither
worker readiness, bypassing Gateway's constructor wrapper, nor bypassing the
proxy eliminated failures. No such change was adopted in product code. The
initial multi-case process stalled after its first result; subsequent trials
were isolated by process to avoid shared-loop reuse confounding the comparison.

Operations was selected to own orphan recovery. The actual cleanup service,
durable execution-to-cluster correlation and recovery deadline remain handoff
prerequisites. Idle termination does not prove recovery of active orphaned work.
Scheduler/worker token automount is disabled, but their container security
contexts are currently unset. The owner explicitly deferred worker hardening, so
that portion of CWL-006 stays open. Existing task-container security results do
not establish worker admission or hardening.

## Local TLS isolation

A plain TCP LocalCluster in the candidate image passed three of three early-client
computations. Local TLS controls reproduced CommClosedError without Calrissian,
Gateway or Kubernetes pod networking. Certificate verification stayed enabled;
temporary test keys were removed afterward. Both images used Python 3.12.15,
Dask/Distributed/Gateway 2026.3.0, Tornado 6.5.9 and msgpack 1.2.1. Their Python
builds and OpenSSL versions differ: candidate OpenSSL 3.5.8, native OpenSSL 3.6.5.

| Local TLS control | Succeeded | Failed |
| --- | ---: | ---: |
| Candidate image, TLS 1.2 | 1 | 2 |
| Candidate image, TLS 1.3 | 1 | 2 |
| Native image, TLS 1.2 | 1 | 2 |
| Native image, TLS 1.3 | 0 | 3 |
| Native image with temporary Tornado 6.6 overlay, TLS 1.2 | 1 | 2 |
| Native image with temporary Tornado 6.6 overlay, TLS 1.3 | 0 | 3 |

This narrows the investigation to a shared runtime/TLS path; it does not identify
Dask, Tornado, CPython or OpenSSL individually as the cause. Switching TLS protocol
or Tornado did not eliminate failures. Product dependency pins and TLS settings
were not changed. The owner explicitly selected investigation of the pinned 2026.3.0 versions only;
no comparison with a newer Dask/Distributed release was performed.

Native-image key-permission setup errors and unavailable Security.temporary
support were excluded from these counts. An independent-loop diagnostic had a
malformed boolean flag caused by text substitution and was excluded from
quantitative results; its logs are retained. The structured control record and
raw logs are under the evidence directory's `policy-phase/` subdirectory.

## Pinned runtime error-state experiment

With Dask/Distributed/Gateway still at 2026.3.0, a local TLS 1.3 diagnostic in
`calrissian-cwl-readiness:430ce62` compared clearing prior thread-local error
state immediately before Tornado's TLS read. Actual read exceptions still
propagated, certificate verification remained enabled, and no task retries
were added.

| Diagnostic treatment | Succeeded | Failed |
| --- | ---: | ---: |
| Unmodified read | 0 | 3 |
| Clear C errno only | 1 | 2 |
| Clear OpenSSL error queue only | 10 | 0 |
| Clear both | 3 | 0 |

The probe observed queued OpenSSL error `0xa000126`, `unexpected eof while reading`,
before a later read. Tracing also captured an ordinary nanny registration
connection closing before an unrelated client/scheduler connection failed.
Together these results strongly support stale OpenSSL error state affecting
subsequent TLS operations. They do not establish exactly which operation first
leaves that state behind, or prove that every cluster disconnection has this cause.

[OpenSSL 3.5 documents](https://docs.openssl.org/3.5/man3/SSL_get_error/)
that the error queue must be empty before the I/O operation examined by
`SSL_get_error`. [CPython issue 81891](https://github.com/python/cpython/issues/81891)
describes missing queue cleanup before SSL I/O. The inspected
[Python 3.12.15 source](https://github.com/python/cpython/blob/v3.12.15/Modules/_ssl.c)
does not clear it immediately before `SSL_read_ex`.

The evidence directory's `pinned-tls/` contains the raw logs,
`error-state-results.json`, and `proposed-cpython-3.12.15-ssl-read.patch`.
The patch adds `ERR_clear_error()` immediately before `SSL_read_ex` as a
runtime experiment. It has **not been compiled, deployed or validated as a
runtime fix**. The diagnostic ctypes hook is not a product workaround.

The owner selected an **operations-provided patched Python runtime**. No runtime
backport will be added to the container build in this work. Operations must
provide the runtime image and its Python/OpenSSL versions and patch provenance.
Dask/Distributed/Gateway remain pinned to 2026.3.0. Before closing the affected
CWL-002/CWL-004/CWL-005 checks, repeat the local TLS control without the diagnostic
hook using that runtime, then the blocked PVC and cancellation probes on
minikube. Those checks remain open pending the supplied runtime and verification.

## Operational decisions and retained evidence

Authentication was explicitly deferred. The initial profile had no Gateway
resource caps and an idle timeout of zero. The approved validation overlay and
successful idle test above supersede that initial state, without establishing
production defaults or active orphan recovery. Operations must provide its
cleanup service, correlation contract and recovery deadline. Worker hardening
was explicitly left open by the owner.

The new cluster initially failed with an inotify instance limit of 128. An
approved temporary increase to 1024 and restart of its networking pods resolved
the startup failure. A later Wi-Fi change required a profile restart before
GitHub resolved correctly. The interrupted Dask run is preserved separately
from the complete passing run. The short follow-log timeout in the temporary
conformance driver was corrected to wait for actual terminal Job/container
states; all final reports reflect completed test executions.

Evidence is repository-relative under
`dask-gateway/verification-results/cwl-readiness-2026-10-31/`:

- `baseline.json`, image identity files and `pod-observations.json` describe the
  candidate and actual running images/settings.
- `required-counts.json`, per-suite XML/logs and `dask-suite/` contain completed
  conformance/lifecycle evidence; `interrupted-dask-suite/` retains the disrupted run.
- RBAC, token, security and network reports record the independent controls.
- Initialization-fault and subsequent PVC/cancellation logs retain both successful
  checks and blocked attempts.

The profile is stopped while decisions are pending, and the host inotify limit
is restored to 128. The final unit suite passed 555 tests, with one skipped and 51 subtests. Focused
mount-module lint/format/type/security checks passed. Strict documentation build
still fails with the same 17 pre-existing warnings; the previously recorded
broader typing errors remain unresolved. Production cluster readiness and these
quality gates are not declared complete by the integration results.
