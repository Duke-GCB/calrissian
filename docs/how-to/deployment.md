# Deploy a standalone runner Job

Use this example on an existing Kubernetes cluster without installing the
Minikube conformance environment. It runs a small CWL tool, saves its outputs
on a shared PVC, and leaves a reader Pod available for retrieving results.

## Prepare the image and storage

You need kubectl access that can create a namespace and namespaced RBAC,
a registry reachable by the cluster, and a StorageClass supporting
ReadWriteMany across the nodes used by the runner and tasks.
Inspect your context and available storage classes:

```sh
kubectl config current-context
kubectl get storageclass
```

Ask your storage administrator which class supports concurrent mounts and
writing as UID 1001. `fsGroup` behavior depends on the CSI driver and backing
filesystem; adjust permissions there if necessary. Do not use this example's
fixed UID unchanged on a cluster that assigns its own permitted UID range.

Build and push the runner from the checkout you want to deploy. Replace the
registry name below with one you can write to:

```sh
export RUNNER_IMAGE=registry.example.org/team/calrissian:smoke-001
docker build -t "$RUNNER_IMAGE" .
docker push "$RUNNER_IMAGE"
```

Use an immutable tag or digest for repeatable deployments. This image and the
CWL tool's `debian:bookworm-slim` image must be pullable by the cluster. Private
registries need pull credentials configured for the relevant runner and workflow
ServiceAccounts. A successful pull by the runner does not configure task pulls.

## Render and submit the manifest

Download [deployment.yaml](files/deployment.yaml), or use the checked-in file.
Set your RWX storage class and render both placeholders:

```sh
export RWX_CLASS=your-rwx-storage-class
sed -e "s|CALRISSIAN_IMAGE|$RUNNER_IMAGE|g" \
    -e "s|RWX_STORAGE_CLASS|$RWX_CLASS|g" \
    docs/how-to/files/deployment.yaml > /tmp/calrissian-deployment.yaml
kubectl apply --dry-run=client -f /tmp/calrissian-deployment.yaml
kubectl apply -f /tmp/calrissian-deployment.yaml
```

The manifest creates a dedicated `calrissian-example` namespace, separate runner
and workflow accounts, management RBAC, a 10 GiB RWX claim, CWL/configuration
files, the runner Job, and a read-only results reader. The runner is its Pod's
first container, matching PVC discovery. Temporary files are also on the PVC.
Task Pods refer to the runner Pod as their owner, allowing Kubernetes garbage
collection to remove them when the runner Pod is deleted.

`backoffLimit: 0` prevents automatic whole-workflow retries. Runner memory
limits are separate from the `--max-ram` task budget. The example is a standard
CWL smoke deployment, without Dask or network-isolation policies. To enforce
CWL NetworkAccess, configure the [matching policies and labels](security.md).

## Monitor both success and failure

```sh
kubectl -n calrissian-example get jobs,pods,pvc
kubectl -n calrissian-example logs -f job/smoke-001
kubectl -n calrissian-example get job smoke-001 -o yaml
```

A successful Job has a `Complete=True` condition. `Failed=True` indicates
failure; inspect its runner logs and Pod termination details. A Job with neither
condition may still be scheduling or running. `kubectl wait --for=condition=complete`
alone does not return early when the Job fails.

Use [troubleshooting](validation.md#diagnose-common-failures) if the claim stays
Pending, images cannot be pulled, or the task fails.

## Retrieve the outputs

Once the Job completes, wait for the reader and inspect the result:

```sh
kubectl -n calrissian-example wait --for=condition=Ready pod/results-reader --timeout=120s
kubectl -n calrissian-example exec results-reader -- cat /shared/smoke-001/results/greeting.txt
kubectl -n calrissian-example exec results-reader -- cat /shared/smoke-001/outputs.json
kubectl -n calrissian-example exec results-reader -- cat /shared/smoke-001/usage.json
```

The greeting is `hello from Calrissian`. Copy all artifacts to the host without
requiring `tar` in the runner image:

```sh
kubectl -n calrissian-example exec results-reader -- python -c \
  'import sys, tarfile; t = tarfile.open(fileobj=sys.stdout.buffer, mode="w|"); t.add("/shared/smoke-001", arcname="smoke-001"); t.close()' \
  > smoke-001.tar
```

The reader runs for 24 hours. Recreate it from the rendered manifest if it has
exited and you still need the files. Results remain on the PVC after Pods exit.
For another run, use a new Job name, task-label value, and output directory;
changing only the Job name would reuse existing paths.

Follow [cancellation and cleanup](cleanup.md) to stop a run or remove resources.
This manifest has been checked structurally against the runtime; it is not a
record of a completed run on your storage driver or cluster.
