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

This will build `calrissian:conformance` from the current source tree. You may need to tag that differently if pushing to a registry. If so, update the `image: ` in [ConformanceTestsJob.yaml](ConformanceTestsJob.yaml)

### Running Conformance Tests

[ConformanceTestsJob.yaml](ConformanceTestsJob.yaml) uses `run_test.sh` from cwltool to run conformance tests with `RUNNER=calrissian` and Calrissian's required arguments in `EXTRA`.

```
kubectl --namespace="$NAMESPACE_NAME" create -f ConformanceTestsJob.yaml
kubectl --namespace="$NAMESPACE_NAME" wait --for=condition=Ready\
   --selector=job-name=conformance-tests pods
kubectl --namespace="$NAMESPACE_NAME" logs -f jobs/conformance-tests
```

### Notes:

- Conformance tests use cwltool-1.0.20181201184214 as that's the nearest version to calrissian's dependency.
- Calrissian requires specifying an envelope of RAM and CPU resources to use in the cluster, so these are provided as `--max-ram` and `--max-cores`
- Since kubernetes is entirely container-based, CWL Tools that do not specify a Docker image will not run unless Calrissian is run with `--default-container`
- This job uses an `initContainer` to guarantee the output volume is writable by the calrissian container. The calrissian image runs as a non-root user.
- This Job sets the `TMPDIR` environment variable to `/outdir`, which is backed by a persistent volume. This is because [cwltest](https://github.com/common-workflow-language/cwltest/blob/683f8d75811f5cb8488a9fa080c97e8ada609cef/cwltest/__init__.py#L70) executes calrissian with  --outdir set to a temp directory provided by Python's  `tempfile.mkdtemp()`. By setting `TMPDIR` to a persistent volume backed-path, we meet Calrissian's requirement that output data is stored to a persistent volume.
- This Job sets the `CALRISSIAN_POD_NAME` variable using the [Kubernetes Downward API](https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/). Calrissian needs to know its pod name at runtime so it can query the API for the names and mount points of persistent volume claims.
