# Supply configuration and environment variables

Use one `--conf settings.json` file for runner defaults, or place defaults
in `~/.calrissian/default.json`. Explicit CLI settings override loaded defaults.
Although the parser accepts repeated `--conf`, only the first file is loaded.
See [configuration precedence](../reference/configuration.md#configuration-precedence).
JSON keys use runtime attribute names:

```json
{
  "max_ram": "4G",
  "max_cores": "2",
  "pod_serviceaccount": "calrissian-workflow",
  "tmpdir_prefix": "/shared/tmp/",
  "tmp_outdir_prefix": "/shared/tmpout/"
}
```

Mount referenced files and shared directories into the runner before execution.
The [CLI reference](../reference/configuration.md) maps option names to JSON keys.

For task environment variables, use `--pod-env-vars environment.yaml` with a YAML
map. To import existing namespace resources, repeat:

```sh
calrissian --max-ram 4G --max-cores 2 \
  --env-from-configmap application-settings \
  --env-from-secret application-credentials workflow.cwl inputs.yml
```

This fragment assumes storage and identity defaults are already configured.
Create those ConfigMaps/Secrets in the task namespace first.
These imports target
the main task container; they do not configure every Dask helper container.
Kubernetes Secret environment injection is separate from CWL `cwltool:Secrets`.
For `cwltool:Secrets`, Calrissian substitutes marked values into task environment
variables, command arguments, and generated staged files. Resolved values are
present in the Kubernetes Pod specification and any staged files while the task
runs. Kubernetes API access, audit or diagnostic logging, and storage access
must be restricted accordingly; Calrissian removes generated secret-bearing
staged files after output collection and when the runner handles task errors.
This cleanup cannot remove copies that a tool writes elsewhere. Calrissian
redacts secret values in runner-captured tool logs, but a tool can still print
or write a secret into its own output, which cannot be reliably prevented by
the runner. For Dask tools, CWL secret environment values are supplied to the
main task container only; they are not propagated to Dask helper or worker
containers. Use Kubernetes Secret environment injection when the workflow
should reference a Kubernetes Secret directly rather than carry the secret as
a CWL input.

Avoid putting credentials in committed
configuration or published logs.

For chart-managed runners, `runnerEnv` values configure the runner process.
Those values are distinct from task environment configuration; for example,
Dask cleanup resource variables must be set on the runner.

## Create a task environment ConfigMap

Create this resource in the task namespace (replace `YOUR_NAMESPACE`):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: application-settings
  namespace: YOUR_NAMESPACE
data:
  SPECIAL_LEVEL: "very"
  SPECIAL_TYPE: "charm"
```

Apply the resource, then pass `--env-from-configmap application-settings` to
Calrissian. This imports its data into the main task container's `envFrom`.
A Secret is selected with `--env-from-secret NAME`; avoid using a ConfigMap
for credentials. To supply an explicit runner-readable YAML environment map,
use `--pod-env-vars /config/environment.yaml`:

```yaml
SPECIAL_LEVEL: "very"
SPECIAL_TYPE: "charm"
```

Use string values and avoid conflicting definitions. The explicit task `env`
entries take precedence over imported `envFrom` entries under Kubernetes
semantics; Calrissian's runtime environment map can also override same-named
values assembled for the tool. Review `EnvVarRequirement` when the environment
setting is part of the portable CWL tool definition.
