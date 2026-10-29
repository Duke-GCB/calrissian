# CLI and JSON configuration

This table is generated from `calrissian.main.add_arguments`. It covers all
Calrissian-specific options; use `calrissian --help` for inherited cwltool
options. Run `task docs:cli` after changing the parser.

## Calrissian options

<!-- generated-options:start -->

| Option | JSON key | Value / repetition | Parser default | Description |
|---|---|---|---|---|
| `--max-ram` | `max_ram` | string | unset | Maximum amount of RAM to use, e.g 1048576, 512Mi or 2G. Follows k8s resource conventions |
| `--max-cores` | `max_cores` | string | unset | Maximum number of CPU cores to use |
| `--max-gpus` | `max_gpus` | string; parser permits an omitted value | unset | Maximum number of GPU cores to use |
| `--pod-labels` | `pod_labels` | string; parser permits an omitted value | unset | YAML file of labels to add to Pods submitted |
| `--pod-env-vars` | `pod_env_vars` | string; parser permits an omitted value | unset | YAML file of environment variables to add at runtime to Pods submitted |
| `--pod-nodeselectors` | `pod_nodeselectors` | string; parser permits an omitted value | unset | YAML file of node selectors to add to Pods submitted |
| `--pod-gpu-nodeselectors` | `pod_gpu_nodeselectors` | string; parser permits an omitted value | unset | YAML file of node selectors to add to Pods submitted that require GPUs |
| `--pod-automount-service-account-token` | `pod_automount_service_account_token` | flag / JSON boolean | false | Explicitly allow automatic service-account API credentials in spawned task pods |
| `--pod-gpu-tolerations` | `pod_gpu_tolerations` | string | unset | YAML toleration list applied only to task pods allocating nvidia.com/gpu |
| `--pod-serviceaccount` | `pod_serviceaccount` | string | unset | ServiceAccount assigned to spawned workflow task pods; defaults to the namespace's default account. Does not configure the Calrissian runner's identity. |
| `--usage-report` | `usage_report` | string; parser permits an omitted value | unset | Output JSON file name to record resource usage |
| `--stdout` | `stdout` | string; parser permits an omitted value | unset | Output file name to tee standard output (CWL output object) |
| `--stderr` | `stderr` | string; parser permits an omitted value | unset | Output file name to tee standard error to (includes tool logs) |
| `--tool-logs-basepath` | `tool_logs_basepath` | string; parser permits an omitted value | unset | Base path for saving the tool logs |
| `--conf` | `conf` | string; repeatable / JSON list | unset | Defines the default values for the CLI arguments |
| `--no-network-access-pod-label` | `no_network_access_pod_labels` | string; parser permits an omitted value | unset | YAML file to set the pod label to use for disabling network access |
| `--network-access-pod-label` | `network_access_pod_labels` | string; parser permits an omitted value | unset | YAML file to set the pod label to use for enabling network access |
| `--dask-gateway-url` | `dask_gateway_url` | string; parser permits an omitted value | unset | Defines the Dask Gateway URL |
| `--dask-script-configmap` | `dask_script_configmap` | string; parser permits an omitted value | unset | Name of the already existing configmap with custom script for dask |
| `--pod-priority-class` | `pod_priority_class` | string; parser permits an omitted value | unset | Priority Class Name assigned to the pod |
| `--env-from-secret` | `env_from_secret` | string; repeatable / JSON list | unset | Secret Id to set the pod environment |
| `--env-from-configmap` | `env_from_configmap` | string; repeatable / JSON list | unset | ConfigMap Id to set the pod environment |

<!-- generated-options:end -->

“Unset” is the parser default, not a promise about downstream behavior.
`--max-ram` and `--max-cores` must have values after configuration is loaded;
`--max-gpus` defaults effectively to zero. The task ServiceAccount defaults to
Kubernetes' namespace default account if omitted. Supply a dedicated workflow
account explicitly. The token-mount flag defaults to false.

All value-taking options should be supplied with explicit values. An option
whose parser permits an omitted value can otherwise resolve to `None`.
File options take paths readable or writable by the runner, not by the host
that submits its Job. Resource budgets accept strings such as `4G`, `512Mi`,
and `2000m` for CPU; GPU budgets are integer device counts.

## Configuration precedence

The implementation currently loads defaults in this order:

1. Parser defaults.
2. `~/.calrissian/default.json`, if present, using the runner process's HOME.
3. The **first** file passed with `--conf`, if any.
4. Explicit CLI arguments.

Although `--conf` is accepted repeatedly, the implementation reads only
`args.conf[0]`. Merge your settings into one file; later `--conf` files are not
layered. This documents current behavior rather than changing it.
JSON keys are parser attribute names, not hyphenated flags. Use JSON booleans
for flags and lists for repeatable Secret/ConfigMap names. Invalid JSON or an
unreadable explicit file prevents configuration from loading. Do not assume
unknown JSON keys are validated as CLI options.

## Complete configuration example

This is a standard CWL run configuration. Mount `/config` and writable `/shared`
paths in the runner and create the named ServiceAccount, ConfigMap, and Secret
in the task namespace first. Optional resource names are illustrative.

```json
{
  "max_ram": "4G",
  "max_cores": "2",
  "max_gpus": "0",
  "pod_serviceaccount": "calrissian-workflow",
  "pod_automount_service_account_token": false,
  "pod_labels": "/config/labels.yaml",
  "pod_env_vars": "/config/environment.yaml",
  "env_from_configmap": ["application-settings"],
  "env_from_secret": ["application-credentials"],
  "tmpdir_prefix": "/shared/run-001/tmp/",
  "tmp_outdir_prefix": "/shared/run-001/tmpout/",
  "outdir": "/shared/run-001/results",
  "stdout": "/shared/run-001/outputs.json",
  "stderr": "/shared/run-001/runner.log",
  "usage_report": "/shared/run-001/usage.json",
  "tool_logs_basepath": "/shared/run-001/tool-logs"
}
```

Create the parent directories, then run inside the configured runner:

```sh
calrissian --conf /config/run.json workflow.cwl inputs.yml
# An explicit CLI budget overrides the file:
calrissian --conf /config/run.json --max-cores 4 workflow.cwl inputs.yml
```

Inherited options include `--outdir`, `--tmpdir-prefix`, `--tmp-outdir-prefix`,
`--provenance`, `--default-container`, `--no-read-only`, and `--no-match-user`.
Their defaults and accepted forms follow the installed cwltool version.
See [task configuration](../how-to/configuration.md),
[storage paths](storage.md), and [security](../how-to/security.md) for examples.
