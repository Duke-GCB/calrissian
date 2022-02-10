# Calrissian - CWL Conformance Tests

This is under development, and will eventually be automated to run in a CI environment. However, the resources here can be used to run [CWL conformance tests](https://github.com/common-workflow-language/common-workflow-language/blob/master/CONFORMANCE_TESTS.md) on a Kubernetes cluster.

### Cluster Preparation

To run the conformance tests, follow the instructions in the **Creating Namespace and Roles** section of [examples/README.md](../examples/README.md). You do not need to create any VolumeClaims from the examples.

### Staging Conformance Tests Data

To prepare a volume containing the conformance test data, create [StageConformanceTestsData.yaml](StageConformanceTestsData.yaml):

```
kubectl --namespace="$NAMESPACE_NAME" create -f StageConformanceTestsData.yaml
```

### Building the container

**This step can probably be simplified, but as conformance tests are in development it's too early to optimize**

Calrissian does not include cwltest, so build a container that installs it.

```
./build-conformance.sh
```

This will build `calrissian:conformance` from the current source tree. You may need to tag that differently if pushing to a registry. If so, update the `image: ` in [ConformanceTestsJob-1.2.yaml](ConformanceTestsJob-1.2.yaml)

### Running Conformance Tests

[ConformanceTestsJob-1.2.yaml](ConformanceTestsJob-1.2.yaml) uses `cwltest` from cwltool to run conformance tests with `--tool calrissian` and Calrissian's required arguments after `--`.

```
kubectl --namespace="$NAMESPACE_NAME" create -f ConformanceTestsJob-1.2.yaml
kubectl --namespace="$NAMESPACE_NAME" wait --for=condition=Ready\
   --selector=job-name=conformance-tests-1-2 pods
kubectl --namespace="$NAMESPACE_NAME" logs -f jobs/conformance-tests-1-2
```

### Inspect the volumes content

Create a pod to inspect the volume with: 

```
kubectl --namespace="$NAMESPACE_NAME"n apply -f inspect-volumes-pod.yaml 
```

Open a shell in the pod with:

```
 kubectl --namespace="$NAMESPACE_NAME" exec --stdin --tty inspect-volumes -- /bin/bash
```

Inpect the content of `/output` with `ls -l /output`

### Notes:

- Calrissian requires specifying an envelope of RAM and CPU resources to use in the cluster, so these are provided as `--max-ram` and `--max-cores`
- Since kubernetes is entirely container-based, CWL Tools that do not specify a Docker image will not run unless Calrissian is run with `--default-container`
- This job uses an `initContainer` to guarantee the output volume is writable by the calrissian container. The calrissian image runs as a non-root user.
- This Job sets the `TMPDIR` environment variable to `/outdir`, which is backed by a persistent volume. This is because [cwltest](https://github.com/common-workflow-language/cwltest/blob/683f8d75811f5cb8488a9fa080c97e8ada609cef/cwltest/__init__.py#L70) executes calrissian with  --outdir set to a temp directory provided by Python's  `tempfile.mkdtemp()`. By setting `TMPDIR` to a persistent volume backed-path, we meet Calrissian's requirement that output data is stored to a persistent volume.
- This Job sets the `CALRISSIAN_POD_NAME` variable using the [Kubernetes Downward API](https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/). Calrissian needs to know its pod name at runtime so it can query the API for the names and mount points of persistent volume claims.
