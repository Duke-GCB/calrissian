# Use the `cwltool:Secrets` extension

Use `cwltool:Secrets` to mark CWL string inputs whose resolved values should be
redacted from Calrissian's runner-captured task logs. Calrissian substitutes
those values where the CWL tool uses them, including environment variables,
command arguments, and generated staged files.

This extension does not create or fetch Kubernetes Secret resources. The
secret values are supplied as regular CWL job inputs, so pass them through a
protected workflow submission path and avoid committing real values in job
files.

## Mark secret inputs

Declare the cwltool namespace and add the extension hint to the process that
uses the secret. The list contains CWL input IDs, not the values themselves:

```yaml
cwlVersion: v1.2
class: CommandLineTool
$namespaces:
  cwltool: http://commonwl.org/cwltool#

hints:
  cwltool:Secrets:
    secrets: [api_token]

inputs:
  api_token: string
```

Mark every process scope that receives the value. In a workflow, list the
secret input on the Workflow and again on the CommandLineTool that receives
the connected input. This keeps the marking explicit across the workflow
boundary.

## Use a marked input

The marked input can be substituted into an environment variable:

```yaml
hints:
  cwltool:Secrets:
    secrets: [api_token]
  EnvVarRequirement:
    envDef:
      - envName: API_TOKEN
        envValue: $(inputs.api_token)
```

Or use it in an argument expression or input binding:

```yaml
arguments:
  - valueFrom: $(inputs.api_token)
```

```yaml
inputs:
  api_token:
    type: string
    inputBinding:
      position: 1
```

For a generated staged file, use an `InitialWorkDirRequirement` entry
expression. The same mechanism supports writable generated files:

```yaml
hints:
  cwltool:Secrets:
    secrets: [api_token]
  InitialWorkDirRequirement:
    listing:
      - entryname: token.txt
        entry: $(inputs.api_token)
```

Include the secret input in the job object supplied to Calrissian:

```yaml
api_token: "example-value-do-not-use-in-production"
```

The example is intentionally fake. Provide real values using your deployment's
protected submission mechanism. See the
[CWL secrets example](../../examples/cwl-secrets/README.md) for a complete
workflow/job pair covering environment values, argument expressions, input
bindings, and read-only and writable staged files.

## Understand the security boundary

Calrissian redacts matching values from runner-captured tool logs. The resolved
values still appear in the task Pod specification for environment variables
and arguments, and in staged files while the task runs. Restrict Kubernetes
API, audit-log, diagnostic, and shared-storage access accordingly. Calrissian
cleans generated secret-bearing staged files after output collection and when
the runner handles task errors; a tool can still copy a secret elsewhere or
print it into its own output, which the runner cannot reliably prevent.

For Dask tools, CWL secret environment values are set in the main task
container; they are not propagated to Dask helper or worker containers.
Kubernetes-native Secret injection with `--env-from-secret` is a separate
option when the task should read a Kubernetes Secret directly. See
[configuration and environment variables](configuration.md) for that option.
