# Work on Calrissian

Start from a checkout and a Python version in the configured CI matrix
(3.10–3.14). Install Hatch and uv in your development environment. Node.js is
needed for CWL JavaScript evaluation; Docker and Kubernetes tooling are needed
for container/integration work.

## Create the environment and run fast checks

Follow the [certificate/bootstrap instructions](../installation.md) for each new
environment before installing dependencies. For example, after activating a
host virtual environment:

```sh
sh scripts/bootstrap-pip.sh
python -m pip install hatch uv
export VIRTUALENV_PIP=26.2.2
hatch run test.py3.12:python -m pytest -q
```

Select an installed Python version in place of `3.12`. Hatch creates the selected
test environment. The project's test environments set `RETRY_ATTEMPTS=0` to
avoid production retry delays in tests.

For a focused change, run the relevant regression module first:

```sh
hatch run test.py3.12:python -m pytest -q tests/test_k8s.py
hatch run test.py3.12:python -m pytest -q tests/test_report.py
```

Then run the complete unit suite. CI builds the workspace distributions and
runs tests across its configured Python matrix; a local test run covers only your
selected interpreter. Build all packages with `hatch build --all` before
reviewing packaging changes.

## Find the implementation

| Area | Main source | Typical changes to check |
|---|---|---|
| CLI/defaults | `main.py`, `context.py` | Parser, configuration precedence, runtime-context propagation |
| Scheduling | `executor.py`, `scheduling.py` | Budgets, callbacks, cancellation, GPU tolerations |
| Task construction | `job.py`, `tool.py` | Commands, mounts, identities, security, environment |
| Kubernetes monitoring | `k8s.py`, `retry.py` | API errors, watch restarts, completion, cleanup |
| Reporting | `report.py` | Units, intervals, totals, serialization |
| Dask extension | `calrissian-dask/` | Entry-point registration, job integration, and lifecycle resources |

See the generated API reference and [diagrams](diagrams.md) for class relationships,
and [execution](../explanation/execution.md) for runtime responsibilities. See
[extension packages](extensions.md) for the plugin hooks and packaging contract.

## Run integration checks when relevant

Use an isolated development cluster. Docker, Minikube, kubectl, Helm, Skaffold,
and Task are required for the checked-in profiles:

```sh
task conformance:cluster
skaffold run
task conformance:test VERSION=1.2.1 ID=107
```

For changes to security or identities, use the
[deployment verification scripts](../how-to/security.md). For Dask changes:

```sh
skaffold run -p dask
task dask:test
task dask:cleanup:test
```

Read the controller output and container verdicts. Saving logs is not a passing
suite verdict. Record the source/image, case IDs, and limitations; use the
[conformance README](https://github.com/Duke-GCB/calrissian/blob/develop/conformance/README.md)
for full-suite runners. Use [cleanup](../how-to/cleanup.md) before removing data
or retrying workflows with external effects.

## Update documentation with the behavior

Install Graphviz (`sudo dnf install graphviz` on Fedora), then run:

```sh
task docs:generate
task docs:serve
```

`docs:generate` refreshes the CLI table, API pages, and UML diagrams. The CLI
reference is checked in; API and UML outputs are ignored. Commit the CLI table
when parser changes alter it. The preview regenerates these assets before serving.
For a build without a server, after generation:

```sh
uv run --no-project --with mkdocs --with mkdocs-material mkdocs build --strict --site-dir /tmp/calrissian-docs
```

Include a regression check for changed behavior, relevant unit/integration
results, and any unverified environment in the pull request. Keep failures and
unsupported behavior visible in conformance evidence.
