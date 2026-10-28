# CLI and JSON configuration

JSON configuration uses attribute names, not hyphenated CLI spellings. Defaults
can be loaded from `~/.calrissian/default.json` or repeatable `--conf` files.
Explicit CLI settings override configured defaults.

| CLI option | JSON key | Meaning |
|---|---|---|
| `--max-ram` | `max_ram` | Aggregate runner RAM budget |
| `--max-cores` | `max_cores` | Aggregate CPU budget |
| `--max-gpus` | `max_gpus` | Aggregate GPU budget |
| `--pod-serviceaccount` | `pod_serviceaccount` | Spawned task identity, not runner identity |
| `--pod-automount-service-account-token` | `pod_automount_service_account_token` | Explicit token opt-in; JSON boolean |
| `--pod-labels` | `pod_labels` | YAML task-label map |
| `--pod-env-vars` | `pod_env_vars` | YAML task-environment map |
| `--pod-nodeselectors` | `pod_nodeselectors` | CPU/general placement map |
| `--pod-gpu-nodeselectors` | `pod_gpu_nodeselectors` | GPU placement map |
| `--pod-gpu-tolerations` | `pod_gpu_tolerations` | YAML toleration list, positive GPU allocations only |
| `--pod-priority-class` | `pod_priority_class` | Existing PriorityClass name |
| `--network-access-pod-label` | `network_access_pod_labels` | YAML allowed-label map |
| `--no-network-access-pod-label` | `no_network_access_pod_labels` | YAML denied-label map |
| `--env-from-configmap` | `env_from_configmap` | Repeatable names; JSON list |
| `--env-from-secret` | `env_from_secret` | Repeatable names; JSON list |
| `--usage-report` | `usage_report` | Resource report JSON destination |
| `--tool-logs-basepath` | `tool_logs_basepath` | Saved task-log base path |
| `--dask-gateway-url` | `dask_gateway_url` | Gateway endpoint |
| `--dask-script-configmap` | `dask_script_configmap` | Existing custom-script ConfigMap |

Other important inherited options include `--outdir`, `--tmpdir-prefix`,
`--tmp-outdir-prefix`, `--provenance`, `--default-container`, `--no-read-only`,
and `--no-match-user`. Consult the installed version's help for their complete
forms. The [cluster guide](../cluster-configuration.md) supplies security examples.
