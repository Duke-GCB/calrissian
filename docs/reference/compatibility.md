# Compatibility and verification scope

Use the package metadata and test evidence from the checkout or release you
intend to run. These constraints describe this source tree; they do not certify
every combination of dependency, Kubernetes version, and storage driver.

| Component | Declared or configured here | Evidence and limits |
|---|---|---|
| Python | `>=3.10`; classifiers through 3.14 | CI is configured to build and run unit tests on 3.10–3.14. The matrix is not evidence of a successful run for every commit. |
| cwltool | `>=3.3.20260925135507,<3.4` | The conformance documentation records the tested source/image. Recheck behavior after dependency updates. |
| Kubernetes Python client | `>=33.1.0,<37` | This is a client dependency range, not a supported Kubernetes server range. |
| Kubernetes server | No server-version support matrix is established here | Recorded complete suites used Minikube; run deployment/security checks on your target server and admission policies. |
| CWL | Suites 1.0.2, 1.1.0, 1.2.0, and 1.2.1 | See [complete results](../conformance/latest-results.md) and [limitations](../conformance/limitations.md), including ENTRYPOINT and ToolTimeLimit. |
| Shared storage | Runner/task paths must map to discovered PVC mounts | Multi-node use normally requires RWX. Single-node results do not validate NFS/CSI permissions, concurrency, or performance on another cluster. |
| Network isolation | Requires matching policies and an enforcing CNI | Test allowed and denied traffic on your cluster; labels alone do not enforce CWL NetworkAccess. |
| GPUs | NVIDIA device allocation and conditional placement settings | Scheduler simulations do not establish CUDA/driver compatibility or hardware performance. |
| Dask extension | Local profile pins Gateway, Dask, and Distributed to 2024.1.0 | Local integration results use simple authentication. Authenticated production deployments require separate verification. |

The authoritative dependency constraints are in
[pyproject.toml](https://github.com/Duke-GCB/calrissian/blob/develop/pyproject.toml).
The Python test matrix is in
[build-and-test.yaml](https://github.com/Duke-GCB/calrissian/blob/develop/.github/workflows/build-and-test.yaml).
Use the corresponding release tag when investigating an older installation.

## Record a deployment baseline

Save these outputs alongside your runner image digest, tool image digests,
StorageClass/CSI version, CNI version, rendered manifests, and workflow inputs:

```sh
kubectl version -o yaml
kubectl get storageclass -o yaml
# Inside the runner environment:
calrissian --version
python --version
python -c 'from importlib.metadata import version; print({p: version(p) for p in ("calrissian", "cwltool", "kubernetes")})'
```

The runtime image does not retain pip, so `importlib.metadata` is useful for
inspecting installed dependencies. Run the [deployment smoke tool](../how-to/deployment.md),
[security checks](../how-to/security.md), and any relevant GPU/Dask tests against
that exact image and cluster. Preserve failures as well as successes.
