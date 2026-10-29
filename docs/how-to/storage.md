# Configure shared storage and temporary paths

Mount PVC-backed input and output/staging paths in the runner. On multi-node
clusters, choose storage that supports the required concurrent access, normally
ReadWriteMany. Single-node Minikube storage is not evidence that a storage class
will work across production nodes.

For example, with a writable PVC mounted at `/shared`:

```sh
mkdir -p /shared/tmp /shared/tmpout /shared/results
calrissian --max-ram 4G --max-cores 2 \
  --tmpdir-prefix /shared/tmp/ \
  --tmp-outdir-prefix /shared/tmpout/ \
  --outdir /shared/results workflow.cwl inputs.yml
```

Run these commands **inside the configured runner**. Inputs consumed by task
pods must also resolve to discovered PVC mounts. Set `CALRISSIAN_POD_NAME` from
`metadata.name` using the downward API. The runner discovers PVC mounts from its
pod's first container; ensure that container is the Calrissian runner.

The task container's `/tmp` uses `emptyDir`. It does not replace runner-side
staging: writable copies and ExpressionTool-produced File literals may need to
be mounted from the runner into another pod. A runner-local staging file can
cause `Could not find a persistent volume mounted for …`.

The conformance runner sets its own `TMPDIR=/output` on a PVC. Explicit prefix
options make the intended staging locations easier to review. Preserve trailing
slashes when you intend a parent directory; see [path semantics](../reference/storage.md).

To use an application with a fixed output path, declare:

```yaml
requirements:
  DockerRequirement:
    dockerPull: example/tool:1.0
    dockerOutputDirectory: /results
```

Replace the example image with your application image. Calrissian mounts each
task's output directory at `/results`, sets its working directory and HOME there,
and collects through the runner's own storage path. A writable output mount
works with a read-only container root filesystem. Mounting over an image
directory hides the original contents at that path.
