# Calrissian

[![Build and test](https://github.com/Duke-GCB/calrissian/actions/workflows/build-and-test.yaml/badge.svg)](https://github.com/Duke-GCB/calrissian/actions/workflows/build-and-test.yaml)
[![PyPI version](https://badge.fury.io/py/calrissian.svg)](https://pypi.org/project/calrissian/)

Calrissian runs [Common Workflow Language (CWL)](https://www.commonwl.org/)
tools and workflows on Kubernetes. It uses `cwltool` to interpret CWL and runs
workflow steps in Kubernetes pods, scheduling independent steps in parallel
within configured CPU and memory budgets.

## Get started

Install Calrissian in the Python environment used to run workflows:

```sh
pip install calrissian
```

Calrissian needs access to a Kubernetes cluster, including OpenShift.
Production deployments should provide shared persistent storage that can be
mounted by the runner and task pods, and permissions scoped to the namespace
where workflows run. See the [installation guide](docs/installation.md) and
[deployment guide](docs/how-to/deployment.md) for requirements and setup.

To try Calrissian on a local Minikube cluster, follow the
[first-tool tutorial](docs/tutorials/first-tool.md).

## Documentation

- [Tutorials](docs/tutorials/index.md) for running tools and workflows.
- [How-to guides](docs/how-to/index.md) for deployment, storage, security, and
  supported extensions.
- [Reference](docs/reference/index.md) for CLI options, configuration, and CWL
  support.
- [Architecture and concepts](docs/explanation/index.md) for execution,
  scheduling, storage, and design.
- [CWL conformance results](docs/conformance/latest-results.md), including
  versioned results and badges.

The rendered documentation is available at
[duke-gcb.github.io/calrissian](https://duke-gcb.github.io/calrissian/).

## Development

See the [contributor guide](docs/development/contributing.md) for environment
setup, tests, integration checks, and documentation tasks. The project uses
Hatch for Python environments and Task for repository workflows.

## License

Calrissian is distributed under the [MIT License](LICENSE).
