# Storage paths

| Setting | Scope | Purpose |
|---|---|---|
| `--outdir` | Runner | Final workflow outputs |
| `--tmp-outdir-prefix` | Runner | Prefix for intermediate task output directories |
| `--tmpdir-prefix` | Runner | Temporary staging; use a shared writable PVC for general compatibility |
| Runner `TMPDIR` | Runner process | Python/default temporary directory selection |
| Task `TMPDIR=/tmp` | Tool container | Per-task emptyDir scratch space |
| `dockerOutputDirectory` | Tool container | Fixed designated output mount path and working directory |

Prefixes are split into a parent directory and a basename prefix:

| Prefix | Generated path |
|---|---|
| `/shared/tmp` | `/shared/tmp<random>` |
| `/shared/tmp/` | `/shared/tmp/<random>` |
| `/shared/tmp-aaa` | `/shared/tmp-aaa<random>` |

Create writable parents first. If a generated directory is itself returned as a
workflow output, its basename may survive final relocation. Define an explicit
output name when the application needs stable naming.

`Could not find a persistent volume mounted for …` means a source path could
not be mapped to a discovered runner PVC. This can occur when File literals or
writable copies are materialized under runner-local `/tmp` and then consumed by
a task pod. The conformance runner uses `TMPDIR=/output` on its output PVC.

Filesystem behavior matters beyond access modes. Azure Files SMB/CIFS metadata
operations and cross-volume relocation require site-specific testing (#124).
