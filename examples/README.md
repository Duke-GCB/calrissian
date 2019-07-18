# Calrissian Examples

These examples demonstrate how to run a CWL workflow in a Kubernetes cluster using Calrissian.

## Cluster Requirements

See the [top-level README](../README.md) for details, but you must have

1. `kubectl` installed and configured to access your cluster (or `oc` if you're using Openshift and prefer `oc`).
2. Ability to create namespaces (if using Kubernetes) or projects (if using Openshift)
2.  A cluster able to provision [PersistentVolumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) with the `ReadWriteMany` access mode.

[Minikube](https://github.com/kubernetes/minikube), [Minishift](https://github.com/minishift/minishift), and [Docker Desktop Kubernetes](https://www.docker.com/products/docker-desktop) support `ReadWriteMany` by default. "Real" clusters may require configuring a StorageClass with NFS, GlusterFS, or another `ReadWriteMany` option listed here [here](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes).

Note that for real workloads, you'll want a real cluster. For these examples, local development clusters work fine.

## Cluster Preparation

### Creating Namespace and Roles

Calrissian executes CWL workflows by running steps as Pods in your cluster. To support this requirement, we create a role with the necessary privileges and bind it to a service account.

Please choose the instructions that match your cluster - you don't need to run both.

#### Kubernetes

```
NAMESPACE_NAME=calrissian-demo-project
kubectl create namespace "$NAMESPACE_NAME"
kubectl --namespace="$NAMESPACE_NAME" create role pod-manager-role \
  --verb=create,patch,delete,list,watch --resource=pods
kubectl --namespace="$NAMESPACE_NAME" create role log-reader-role \
  --verb=get,list --resource=pods/log
kubectl --namespace="$NAMESPACE_NAME" create rolebinding pod-manager-default-binding \
  --role=pod-manager-role --serviceaccount=${NAMESPACE_NAME}:default
kubectl --namespace="$NAMESPACE_NAME" create rolebinding log-reader-default-binding \
  --role=log-reader-role --serviceaccount=${NAMESPACE_NAME}:default
```

#### Openshift

```
oc new-project calrissian-demo-project
oc create role pod-manager-role --verb=create,delete,list,watch --resource=pods
oc create role log-reader-role --verb=get,list --resource=pods/log
oc create rolebinding pod-manager-default-binding --role=pod-manager-role \
  --serviceaccount=calrissian-demo-project:default
oc create rolebinding log-reader-default-binding --role=log-reader-role \
  --serviceaccount=calrissian-demo-project:default
```

### Creating Volumes

We will also create some volume claims to house the data used and generated when running a workflow.

#### Kubernetes

```
kubectl --namespace="$NAMESPACE_NAME" create -f VolumeClaims.yaml
```

#### Openshift

```
oc create -f VolumeClaims.yaml
```

### Staging Input Data

Calrissian expects to load CWL documents, input data, and job orders from a persistent volume. The previous step created a Persistent Volume Claim named `calrissian-input-data` to house these objects. The volume claimed is initially empty, so we run a job to copy data onto it. In this example, files are copied out of the Docker image, but for real world usage, you can populate the input volume any way you like.

To populate the `calrissian-input-data` create a Kubernetes Job using [StageInputDataJob.yaml](StageInputDataJob.yaml).

#### Kubernetes

```
kubectl --namespace="$NAMESPACE_NAME" create -f StageInputDataJob.yaml
```

#### Openshift

```
oc create -f openshift/StageInputDataJob.yaml
```

### Running a Workflow

[CalrissianJob-revsort.yaml](CalrissianJob-revsort.yaml) runs a workflow using Calrissian in a [Kubernetes Job](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/). The workflow, [revsort-array.cwl](../input-data/revsort-array.cwl)), is simple, but easily parallelizable. It reverses the contents of 5 text files, sorts each of them individually, and places the results in the `calrissian-output-data` volume. It also produces a report of resource usage.

The below commands will create the job and follow its logs once it starts. Use `Ctrl+C` to exit after the job completes.

#### Kubernetes

```
kubectl --namespace="$NAMESPACE_NAME" create -f CalrissianJob-revsort.yaml
kubectl --namespace="$NAMESPACE_NAME" wait pod --for=condition=Ready \
  --selector=job-name=calrissian-revsort-array
kubectl --namespace="$NAMESPACE_NAME" logs -f jobs/calrissian-revsort-array
```

#### Openshift

```
oc create -f CalrissianJob-revsort.yaml
oc wait --for=condition=Ready --selector=job-name=calrissian-revsort-array
oc logs -f jobs/calrissian-revsort-array
```

### Viewing Results

TBD

### Job Cleanup

Calrissian will delete completed pods for individual steps, but you may delete the `calrissian-revsort-array` job after running it. The data and any redirected logs will remain in the persistent volume.

#### Kubernetes

```
kubectl --namespace="$NAMESPACE_NAME" delete -f CalrissianJob-revsort.yaml
```

#### Openshift

```
oc delete -f CalrissianJob-revsort.yaml
```
