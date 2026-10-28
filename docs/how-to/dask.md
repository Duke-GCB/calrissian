# Run the Dask Gateway extension

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

- [Extension schema](https://github.com/Duke-GCB/calrissian/blob/develop/calrissian/dask/custom_schema/schema.yaml).
- [Minimal computation fixture](https://github.com/Duke-GCB/calrissian/blob/develop/dask-gateway/tests/minimal.cwl).

`NetworkAccess: true` expresses tool intent. Cluster policy must also allow
connections to Gateway, scheduler, and workers.

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
