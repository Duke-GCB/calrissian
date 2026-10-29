# Configure an authenticated Dask Gateway integration

The repository's Dask profile is a local integration harness using simple
authentication. Upstream calls that authentication mode testing-only. This guide
works through the Calrissian configuration for an **existing JupyterHub-authenticated
Gateway**; it does not install JupyterHub or establish a verified production
configuration for your cluster. Follow the Gateway version's
[server authentication instructions](https://gateway.dask.org/authentication.html)
to register and configure its JupyterHub service first.

## Establish the server and image contract

Your administrator must provide a reachable HTTPS Gateway endpoint, a valid
JupyterHub API token for the submitting identity, a trusted TLS chain, and a
Gateway backend configured to accept the cluster options `image`, `worker_cores`,
`worker_cores_limit`, and `worker_memory`. The built-in initialization script sets
those option names; an arbitrary Gateway installation may not expose them.

Build task and worker images with compatible Dask, Distributed, and Gateway
clients. The repository's tested local profile uses 2024.1.0. Consult the target
Gateway release documentation before changing versions. Its server backend
must permit the chosen worker image and resource values.

Storage and network settings are separate for the Calrissian task, Gateway
scheduler, and workers. Any files read by distributed workers need paths and
mounts configured in the Gateway backend; mounting a PVC in the runner does not
configure Gateway-managed pods. Permit DNS, authenticated HTTPS requests, and
the scheduler/worker connections required by your Gateway/proxy deployment.
Keep server-side resource quotas and cluster lifetime controls in place.

## Supply credentials to all lifecycle containers

The built-in init and disposal scripts call `Gateway(gateway_url)`. One explicit
integration approach is to supply custom versions using upstream's
[JupyterHubAuth client](https://gateway.dask.org/api-client.html):

```python
from dask_gateway import Gateway
from dask_gateway.auth import JupyterHubAuth
import os

gateway = Gateway(
    gateway_url,
    auth=JupyterHubAuth(api_token=os.environ["GATEWAY_API_TOKEN"]),
)
```

Copy both checked-in scripts and replace their Gateway construction with the
above authentication argument. Keep the rest of their lifecycle logic:

```sh
mkdir -p /tmp/calrissian-gateway-scripts
cp calrissian/dask/init-dask.py calrissian/dask/dispose-dask.py /tmp/calrissian-gateway-scripts/
# Edit both copies to import JupyterHubAuth and pass auth= as shown above.
kubectl -n YOUR_NAMESPACE create configmap authenticated-dask-scripts \
  --from-file=/tmp/calrissian-gateway-scripts/init-dask.py \
  --from-file=/tmp/calrissian-gateway-scripts/dispose-dask.py
```

Configure `--dask-script-configmap authenticated-dask-scripts`. The ConfigMap
holds code, not credentials. Initialization must still write the cluster name
to `DASK_CLUSTER_NAME_PATH`; disposal must wait for `/shared/completed`, read that
name, and shut down the matching cluster. Main computation can read `DASK_CLUSTER`
and use the same authenticated Gateway construction to connect.

Place the following YAML in a Kubernetes Secret key named `environment.yaml`,
using your organization's secret provisioning system:

```yaml
GATEWAY_API_TOKEN: "replace-with-the-submitting-user-token"
```

Mount that Secret read-only into the **runner**, for example at
`/credentials/environment.yaml`, and pass:

```sh
calrissian --max-ram 4G --max-cores 2 \
  --pod-serviceaccount calrissian-workflow \
  --dask-gateway-url https://gateway.example.org \
  --dask-script-configmap authenticated-dask-scripts \
  --pod-env-vars /credentials/environment.yaml \
  --tmpdir-prefix /shared/tmp/ --tmp-outdir-prefix /shared/tmpout/ \
  --outdir /shared/results computation.cwl inputs.yml
```

This command assumes the normal runner PVC/RBAC setup and a CWL tool declaring
DaskGatewayRequirement with a compatible image. Dask runners also need the
ConfigMap permissions documented in the [cluster guide](../cluster-configuration.md).
Use `--pod-env-vars` deliberately: the generated environment is passed to Dask
init/main/cleanup containers, whereas `--env-from-secret` imports apply only to
the main container. Explicit environment values appear in Pod specifications;
restrict Pod read permissions and avoid publishing these specs or credentials.
Do not put real tokens in CWL inputs, ConfigMaps, committed files, or example logs.

If your credential policy requires Secret references in every container rather
than explicit Pod environment values, the current CLI does not provide that
helper-container injection contract. Treat it as an integration limitation;
customizing the scripts alone does not add Secret volume mounts.

## Verify and inspect orphaned clusters

Before using real workloads, check that the token can list clusters, initialize
a small cluster, execute one computation, and shut it down. Also run a tool that
fails after initialization and confirm that its cluster is removed while the
failure remains visible. Repeat with the network policies enabled. The local
`task dask:test` profile is separate evidence and does not validate this auth setup.

From an authorized client environment with the same credentials:

```python
import os
from dask_gateway import Gateway
from dask_gateway.auth import JupyterHubAuth

gateway = Gateway(
    os.environ["GATEWAY_URL"],
    auth=JupyterHubAuth(api_token=os.environ["GATEWAY_API_TOKEN"]),
)
for report in gateway.list_clusters():
    print(report.name, report.status)
```

Match a cluster to the run's init log and recorded cluster name before stopping
it. If the task still exists, the name is held in its `/shared/dask_cluster_name.txt`;
this is an `emptyDir`, so deleting the Pod removes that record. After confirming
the run has stopped, shut down only that cluster:

```python
cluster_name = os.environ["ORPHAN_CLUSTER_NAME"]
gateway.connect(cluster_name).shutdown()
```

Recheck the server's cluster state. User credentials normally expose that user's
clusters; account-wide orphan investigation requires the Gateway administrator's
procedure. Upstream documents the
[list/connect/shutdown lifecycle](https://gateway.dask.org/usage.html).
Retain the run-to-cluster mapping before deleting task pods, and configure
server-side lifetime limits for failures where cleanup never runs.
