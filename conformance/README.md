# Calrissian - CWL Conformance Tests

## Conformance tests for CWL v1.0 for the latest release

Coming soon

## Conformance tests for CWL v1.1 for the latest release

Coming soon

## Conformance tests for CWL v1.2.0 for the latest release

### Classes

![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/workflow.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/command_line_tool.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/expression_tool.json?icon=commonwl)

### Required features

![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/required.json?icon=commonwl)

### Optional features

![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/conditional.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/docker.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/env_var.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/format_checking.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/initial_work_dir.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/inline_javascript.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/inplace_update.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/input_object_requirements.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/multiple.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/multiple_input.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/networkaccess.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/resource.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/scatter.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/schema_def.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/secondary_files.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/shell_command.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/step_input.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/step_input_expression.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/subworkflow.json?icon=commonwl)
![test result](https://flat.badgen.net/https/raw.githubusercontent.com/Terradue/calrissian/conformance-1.2.1/conformance/badges/timelimit.json?icon=commonwl)







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

### Copy the badges

```
kubectl cp $NAMESPACE_NAME/inspect-volumes:/output/badges/ badges/
```

### Notes:

- Calrissian requires specifying an envelope of RAM and CPU resources to use in the cluster, so these are provided as `--max-ram` and `--max-cores`
- Since kubernetes is entirely container-based, CWL Tools that do not specify a Docker image will not run unless Calrissian is run with `--default-container`
- This job uses an `initContainer` to guarantee the output volume is writable by the calrissian container. The calrissian image runs as a non-root user.
- This Job sets the `TMPDIR` environment variable to `/outdir`, which is backed by a persistent volume. This is because [cwltest](https://github.com/common-workflow-language/cwltest/blob/683f8d75811f5cb8488a9fa080c97e8ada609cef/cwltest/__init__.py#L70) executes calrissian with  --outdir set to a temp directory provided by Python's  `tempfile.mkdtemp()`. By setting `TMPDIR` to a persistent volume backed-path, we meet Calrissian's requirement that output data is stored to a persistent volume.
- This Job sets the `CALRISSIAN_POD_NAME` variable using the [Kubernetes Downward API](https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/). Calrissian needs to know its pod name at runtime so it can query the API for the names and mount points of persistent volume claims.
