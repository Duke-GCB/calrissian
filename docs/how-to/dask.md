# Run the Dask Gateway extension

Install `calrissian-dask` in the Calrissian runner environment to enable the
extension. The standard Python `calrissian` distribution does not include it:

```sh
python -m pip install calrissian-dask
```

The task image must provide a compatible `dask-gateway` client; it does not
need the `calrissian-dask` package. The standard `ghcr.io/duke-gcb/calrissian`
image contains the Calrissian runtime without the extension. Use
`ghcr.io/duke-gcb/calrissian-dask` for a runner image with the extension already
installed.

The default initialization script runs inside the task image and is
self-contained: it imports only the standard library and `dask-gateway`.
Calrissian supplies its settings through environment variables, including the
optional `DASK_GATEWAY_PVC_MOUNTS` descriptor. PVC propagation does not require
a custom lifecycle ConfigMap or installing the runner extension in the task.

Calrissian can provision a Dask cluster for a tool declaring its
`DaskGatewayRequirement` extension. This is separate from standard CWL conformance.
The [architecture](../explanation/architecture.md) distinguishes Calrissian task
pods from Gateway-managed scheduler and worker pods.

## Prepare the runner

Use the local integration profile to deploy Gateway and a matching worker image:

```sh
task conformance:cluster
skaffold run -p dask
```

The profile uses simple authentication for local development. For production,
configure Gateway authentication, storage, and network policy for your cluster.
See the [authenticated Gateway integration](dask-production.md) for a worked
configuration approach, helper-container credential limits, and orphan cleanup.

Configure the Calrissian runner with:

- `--dask-gateway-url`: the Gateway endpoint reachable from task pods.
- `--dask-script-configmap`: an existing ConfigMap when overriding the default
  initialization and disposal scripts.
- A compatible default container or tool image containing the required clients.

The profile disables the chart's ordinary network-isolation policies so tasks
can reach Gateway. Task security settings and GPU tolerations apply to
Calrissian task pods. Gateway scheduler/worker pods need their own backend settings.

## Declare the extension in CWL v1.2

This valid YAML example illustrates cluster initialization around a small tool.
It runs a local echo command; use the compatibility-suite fixtures linked below
for actual Distributed computation. Supply the runner's Gateway URL and the
appropriate integration image/default container when executing it.

```yaml
cwlVersion: v1.2
class: CommandLineTool
$namespaces:
  calrissian: https://calrissian-cwl.github.io/schema#
requirements:
  ResourceRequirement:
    coresMin: 1
    ramMin: 512
  NetworkAccess:
    networkAccess: true
  calrissian:DaskGatewayRequirement:
    workerCores: 1
    workerCoresLimit: 1
    workerMemory: "256M"
    clusterMaxCores: 1
    clusterMaxMemory: "256M"
inputs: []
baseCommand: [echo, "cluster initialization completed"]
stdout: result.txt
outputs:
  result:
    type: File
    outputBinding:
      glob: result.txt
```

The extension fields describe worker/cluster allocations:

| Field | Meaning |
|---|---|
| `workerCores` | CPU cores requested per worker |
| `workerCoresLimit` | CPU limit per worker |
| `workerMemory` | Worker memory quantity |
| `clusterMaxCores` | Cluster-wide CPU budget used by the extension |
| `clusterMaxMemory` | Cluster-wide memory budget used by the extension |

The schema allows expressions for these fields. For implementation details, see:

- [Extension schema](https://github.com/Duke-GCB/calrissian/blob/develop/calrissian-dask/src/calrissian_dask/resources/schema.yaml).
- [Minimal computation fixture](https://github.com/Duke-GCB/calrissian/blob/develop/dask-gateway/tests/minimal.cwl).

Gateway-based Dask tools must explicitly declare
`NetworkAccess: {networkAccess: true}`. `DaskGatewayRequirement` does not grant
network access implicitly. In CWL v1.2, an omitted NetworkAccess requirement is
handled as false by Calrissian. With enforcing policies, that blocks initialization
and cleanup as well as computation because the containers share a Pod network
namespace. Cluster policy must also allow connections to Gateway, scheduler and
workers. The Dask fixtures declare the permission explicitly; ordinary denied
network tests retain their restrictions.

## Validate server resource and image policies

`dask-gateway/readiness-values.yaml` is an opt-in validation overlay, not a
production default. Layer it after the base Gateway values and any operator PVC
profile when reproducing the candidate readiness checks. It approves only
`calrissian-cwl-readiness:430ce62` and `calrissian-pvc-probe:cohort` as worker
images, caps each worker at one CPU/1 GiB and the worker pool at two workers,
two CPU and 2 GiB, and enables a 60-second idle timeout.

Gateway's aggregate cluster caps include scheduler limits. This overlay includes
the base profile's additional one CPU/512 MiB scheduler limit; increasing these
scheduler limits requires reviewing the arithmetic. The worker budget remains
two CPU/2 GiB. The server rejects unapproved images, oversized workers and caller
attempts to override the idle/worker caps. A scale request above the pool limit
is clamped with a warning rather than provisioning additional workers.

Idle termination covers idle clusters. Operations owns recovery when task/node
loss prevents disposal and work remains active. Operations must retain a durable
execution-to-cluster mapping and define its recovery service and deadline before
that failure path can be signed off. The current configuration does not install
an orphan reaper or establish that production guarantee.

See the [candidate readiness record](../reference/cwl-readiness-2026-10-31.md)
for measured results and the unresolved client-connection probes.

## Configure cleanup resources

The cleanup sidecar has independent CPU and memory requests/limits. Set these
variables on the **runner**, which builds the task pod:

- `CALRISSIAN_DASK_CLEANUP_CPU_REQUEST`
- `CALRISSIAN_DASK_CLEANUP_MEMORY_REQUEST`
- `CALRISSIAN_DASK_CLEANUP_CPU_LIMIT`
- `CALRISSIAN_DASK_CLEANUP_MEMORY_LIMIT`

See [environment reference](../reference/environment.md) for defaults.
These settings do not change Gateway worker budgets.

Initialization runs before the main command. After that command terminates,
a cleanup sidecar attempts to stop the recorded cluster.

- Successful cleanup does not turn a failed tool into a successful result.
- Node loss, pod termination, or an unavailable Gateway can interrupt cleanup.
- Monitor Gateway and keep a procedure for removing orphaned clusters.

## Verify the integration

Run the extension compatibility suite after deployment:

```sh
task dask:test
# One case:
task dask:test ID=3
# Cleanup resource and lifecycle check:
task dask:cleanup:test
```

The suite checks computation and cluster cleanup separately. It is distinct
from official CWL conformance testing. For case details and artifacts, see the
[compatibility-suite reference](https://github.com/Duke-GCB/calrissian/blob/develop/dask-gateway/README.md#cwl-extension-compatibility-suite).

## Share staged files with Gateway workers

Add `--dask-mount-task-pvcs` to the Calrissian command when workers need to read
staged files or write distributed output files. It is disabled by default and
requires a compatible Gateway profile. Application CWL inputs do not change.

The runner already discovers PVCs through `CALRISSIAN_POD_NAME`. The extension
uses each task's resolved mount mappings, including its container paths and PVC
subpaths, rather than copying the runner's mount paths. It forwards only
PVC-backed mounts, preserving read-only restrictions. ConfigMaps, Secrets,
service-account credentials and emptyDir volumes are not propagated. The worker
identity must have filesystem permissions to access the selected files.

The transport is `DASK_GATEWAY_PVC_MOUNTS` in the initialization hook and the
Gateway string option `worker_pvc_mounts`. The JSON contract is defined in
`calrissian-dask/src/calrissian_dask/resources/pvc-mounts.schema.json`.
Custom initialization hooks selected through `--dask-script-configmap` must pass
that environment value to `cluster_options.worker_pvc_mounts` when present.
Unsupported profiles must fail rather than silently dropping the setting.

### Configure the Gateway profile

Gateway must have `jsonschema>=4.23,<5` installed; an optional image recipe is
`dask-gateway/Dockerfile.pvc-profile`. From the checkout root, generate a helper
for the PVCs explicitly approved by operations:

```sh
python dask-gateway/render-pvc-config.py --allow-claim EXECUTION_PVC > /tmp/pvc-profile.py
```

Supply it as an additional Gateway configuration fragment, for example with
Helm `--set-file gateway.extraConfig.pvc-mounts=/tmp/pvc-profile.py` alongside
`dask-gateway/values.yaml`. That values file declares `worker_pvc_mounts` and calls
the helper only when requested. The helper validates the schema, approved claims,
paths and collisions and merges the worker configuration. Other Gateway profiles
must provide equivalent handling, including preserving their existing overrides.
Claim approval must follow the deployment's user/namespace isolation policy; the
example allowlist is intended for the local integration profile.

The feature does not provision storage. Use storage suitable for the worker
placement, normally RWX across nodes. An unresolved `subPathExpr` is rejected.
The scheduler and Gateway API server do not receive these PVC mounts.

### Run the small file-read probe

`dask-gateway/tests/pvc-read.cwl` submits a file read to a worker and asserts its
contents. `pvc-read-job.yaml` supplies `pvc-input.txt`. The result records the
worker address, cluster name and identical staged-file path.

Build the current wheels and the probe image before loading it into minikube:

```sh
uv build --wheel --out-dir dist .
uv build --wheel --out-dir calrissian-dask/dist calrissian-dask
docker build -f dask-gateway/Dockerfile.pvc-probe -t calrissian-pvc-probe:cohort .
minikube -p YOUR_PROFILE image load calrissian-pvc-probe:cohort
```

From a Calrissian runner using this feature image, with the probe files staged
on its PVC and a compatible Gateway profile, run:

```sh
calrissian --dask-gateway-url YOUR_GATEWAY_URL \
  --dask-mount-task-pvcs --max-cores 1 --max-ram 2G \
  --tmpdir-prefix PVC_PATH/tmp/ --tmp-outdir-prefix PVC_PATH/tmpout/ \
  --outdir PVC_PATH/result \
  dask-gateway/tests/pvc-read.cwl dask-gateway/tests/pvc-read-job.yaml
```

Add deployment-specific lifecycle ConfigMap and service-account arguments where
required. The worker must return the expected file content; a success in the
runner alone does not verify propagation. Inspect both task and worker mounts
and confirm cluster cleanup. The local minikube result is recorded in
[the PVC probe verification](../reference/dask-pvc-probe-2026-10-31.md).


## Use the published core wheel for plugin development

In the sibling openEO workspace, the plugin's `local-wheels` group selects
`calrissian==0.19.1` from the explicit `harbor` uv index. The development endpoint
is `http://localhost:8095/cr.terradue.com/python-oci-develop/`. Keep credentials
in the workspace `harbor.env`; the root Taskfile maps them to uv's named-index
variables. From the workspace root:

```sh
task pyoci:start
task uv:sync PROJECT=calrissian/calrissian-dask
```

For direct uv commands, export `UV_INDEX_HARBOR_USERNAME` and
`UV_INDEX_HARBOR_PASSWORD` first. No committed uv.lock is introduced; normal uv
sync commands may create local locks. The normal distribution requirement remains
`calrissian>=0.19.1`, and the registry group selects the approved built baseline.

Core tests remain a deliberate exception: their existing Hatch workspace installs
the plugin from source. Keep using the core's configured Hatch checks for those
tests. Diagnostic runner image builds in `../dask-calrissian-example` use
`task wheels:runner` to build core/plugin artifacts in their own `dist/`
directories, publish that exact cohort to Harbor, then download and verify it for
Docker. Build tasks load the workspace credentials without copying them into images.
