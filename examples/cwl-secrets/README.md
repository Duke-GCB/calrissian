# CWL secrets examples

`verify-secrets-workflow.cwl` exercises the `cwltool:Secrets` input list at both
Workflow and CommandLineTool scope. Its child tool verifies each value without
printing it:

| CWL use | Example |
|---|---|
| Environment value | `EnvVarRequirement.envValue` expression |
| Command argument expression | `arguments[].valueFrom` |
| Input-bound command argument | `inputs[].inputBinding` |
| Generated staged files | Read-only and writable `InitialWorkDirRequirement` entry expressions |

`job.yml` contains fake demonstration strings only; do not replace them with
real credentials. The tool compares secret values by SHA-256 digest and never
prints the values. `cwltool:Secrets` marks CWL input IDs on the process that
receives them. In a workflow, mark workflow inputs and mark the corresponding
tool inputs as shown here.

The CWL examples are valid with cwltool's registered `cwltool:Secrets` hint.
cwltool may report that extension as an unknown hint during validation; it
still validates the CWL and handles the extension during execution.

## Run on the conformance Minikube cluster

The conformance chart provides a `calrissian-session` runner and shared RWX
`/output` volume. From the repository root, stream the examples and this
checkout's Calrissian source into the runner pod, then invoke the workflow
there. Streaming avoids requiring `tar` in the minimal runner image. This uses
the current source changes without rebuilding the image; the task image is the
conformance image already loaded in Minikube.

```sh
CONTEXT=calrissian-conformance
NAMESPACE=calrissian-conformance
POD=$(kubectl --context "$CONTEXT" --namespace "$NAMESPACE" \
  get pods -l app=calrissian-session -o jsonpath='{.items[0].metadata.name}')
TASK_IMAGE=$(kubectl --context "$CONTEXT" --namespace "$NAMESPACE" \
  get deployment calrissian-session \
  -o jsonpath='{.spec.template.spec.containers[0].image}')

tar -czf - calrissian examples/cwl-secrets | \
  kubectl --context "$CONTEXT" --namespace "$NAMESPACE" exec -i "$POD" -- \
    python3 -c 'import sys,tarfile; tarfile.open(fileobj=sys.stdin.buffer, mode="r|gz").extractall("/tmp")'
kubectl --context "$CONTEXT" --namespace "$NAMESPACE" exec "$POD" -- sh -c \
  "PYTHONPATH=/tmp calrissian --debug --max-ram 1G --max-cores 1 \\
   --default-container '$TASK_IMAGE' \\
   --tool-logs-basepath /output/cwl-secrets-logs \\
   --outdir /output/cwl-secrets-results \\
   /tmp/examples/cwl-secrets/verify-secrets-workflow.cwl \\
   /tmp/examples/cwl-secrets/job.yml > /tmp/examples/cwl-secrets/calrissian.log 2>&1"
```

Check that the tool succeeded, both generated secret files were cleaned up,
and neither runner nor tool logs contain the fake secret values:

```sh
kubectl --context "$CONTEXT" --namespace "$NAMESPACE" exec "$POD" -- sh -c \
  'grep -Fx "CWL secret substitution checks passed" /output/cwl-secrets-results/verification.txt && \
   test ! -e /output/cwl-secrets-results/staged-secret.txt && \
   test ! -e /output/cwl-secrets-results/writable-staged-secret.txt && \
   ! grep -E "example-(env-secret|argument-secret|input-binding-secret|staged-file-secret|writable-staged-file-secret)" \
     /tmp/examples/cwl-secrets/calrissian.log && \
   ! grep -E "example-(env-secret|argument-secret|input-binding-secret|staged-file-secret|writable-staged-file-secret)" \
     /output/cwl-secrets-logs/verify.log'
```

The resolved environment and arguments necessarily appear in the task Pod
specification, and the staged file exists while the task runs. Restrict access
to the Kubernetes API and shared volume. The tool deliberately emits only a
success message; runner redaction cannot protect a tool that prints or copies a
secret into its output.
