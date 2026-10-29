# CWL support and evidence

Calrissian uses cwltool's CWL interpretation with a Kubernetes task backend.
The repository stages suites 1.0.2, 1.1.0, 1.2.0, and 1.2.1. The new tutorials
use CWL v1.2; the 1.2.1 patch suite also uses the `v1.2` language identifier.

See [latest recorded full runs](../conformance/latest-results.md) and the
[current limitations](../conformance/limitations.md) for exact images and results.
The complete-run totals describe their recorded image and cluster configuration;
they do not claim full conformance.

| Feature | Scope / limitation |
|---|---|
| CommandLineTool, workflows, scatter, ExpressionTool | Conformance coverage exists; generated files still need shared staging |
| DockerRequirement | `dockerPull` and `dockerOutputDirectory`; required other fields rejected |
| Image ENTRYPOINT | Known compatibility gap, #207 |
| NetworkAccess | Labels plus operator-installed enforcing policy; documented literal requirement coverage |
| ToolTimeLimit | Required feature explicitly unsupported |
| cwltool:Secrets | Substitutes secret placeholders in task environment variables, command arguments, and staged generated files. Runner logs redact matching values; Pod specs and staged files contain resolved values during execution. Tool-produced output can still expose a secret. |
| CUDARequirement | NVIDIA resource allocation/placement; actual hardware and drivers are deployment responsibilities |
| DaskGatewayRequirement | Calrissian extension with a separate compatibility suite |

For new failures include the image/commit, suite version and test number, runner
configuration, and task events/logs. Do not infer complete cloud/storage support
from a local conformance pass.
