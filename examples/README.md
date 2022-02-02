# Calrissian Examples

These examples demonstrate how to run a CWL workflow in a Kubernetes cluster using Calrissian.

## Requirements

See the [top-level README](../README.md) for Cluster Requirements. To run these examples specifically but you must have:

1. `kubectl` installed and configured to access your cluster (or `oc` if you're using Openshift and prefer `oc`).
2. Ability to create and use namespaces (if using Kubernetes) or projects (if using Openshift)
3. A cluster able to provision [PersistentVolumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) with the `ReadWriteMany` access mode.

[Minikube](https://github.com/kubernetes/minikube), [Minishift](https://github.com/minishift/minishift), and [Docker Desktop Kubernetes](https://www.docker.com/products/docker-desktop) support `ReadWriteMany` by default. "Real" clusters may require configuring a StorageClass with NFS, GlusterFS, or another `ReadWriteMany` option listed here [here](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes).

Note that for real workloads, you'll want a real cluster. For these examples, local development clusters work fine.

## Cluster Preparation

### Creating Namespace and Roles

Calrissian executes CWL workflows by running steps as Pods in your cluster. To support this requirement, we create a role with the necessary privileges and bind it to a service account.
By default, the created steps pods are therefore executed with the serviceaccount specified in the Calrissian job. If not specified, the default service account of the namespace is used.
The service account for the step pods can be overridden by specifying the `--pod_serviceaccount` option.
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
oc create -f StageInputDataJob.yaml
```

### Running a Workflow

[CalrissianJob-revsort.yaml](CalrissianJob-revsort.yaml) runs a workflow using Calrissian in a [Kubernetes Job](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/). The workflow, [revsort-array.cwl](../input-data/revsort-array.cwl)), is simple, but easily parallelizable. It reverses the contents of 5 text files, sorts each of them individually, and places the results in the `calrissian-output-data` volume. It also produces a report of resource usage.

The below commands will create the job and follow its logs once it starts.

#### Kubernetes

```
kubectl --namespace="$NAMESPACE_NAME" create -f CalrissianJob-revsort.yaml
# Wait for job to start ...
kubectl --namespace="$NAMESPACE_NAME" logs -f jobs/calrissian-revsort-array
```

Use `Ctrl+C` to exit after the job completes.

#### Openshift

```
oc create -f CalrissianJob-revsort.yaml
# Wait for job to start ...
oc logs -f jobs/calrissian-revsort-array
```

Use `Ctrl+C` to exit after the job completes.

### Viewing Results

Calrissian will print the CWL Job output JSON to the logs, but output files, logs, and reports are stored on the output volume. Run [ViewResultsJob.yaml](ViewResultsJob.yaml) to see them

#### Kubernetes

```
kubectl --namespace="$NAMESPACE_NAME" create -f ViewResultsJob.yaml
# Wait for job to start ...
kubectl --namespace="$NAMESPACE_NAME" logs -f jobs/view-results
```

Use `Ctrl+C` to exit after the job completes.

#### Openshift

```
oc create -f ViewResultsJob.yaml
# Wait for job to start ...
oc logs -f jobs/view-results
```

### Job Cleanup

Calrissian will delete completed pods for individual steps, but you may want to delete the  jobs after running them. The data and any redirected logs will remain in the persistent volume.

#### Kubernetes

```
kubectl --namespace="$NAMESPACE_NAME" delete -f StageInputDataJob.yaml
kubectl --namespace="$NAMESPACE_NAME" delete -f CalrissianJob-revsort.yaml
kubectl --namespace="$NAMESPACE_NAME" delete -f ViewResultsJob.yaml
```

#### Openshift

```
oc delete -f StageInputDataJob.yaml
oc delete -f CalrissianJob-revsort.yaml
oc delete -f ViewResultsJob.yaml
```
