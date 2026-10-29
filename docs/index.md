# Calrissian

Calrissian executes Common Workflow Language (CWL) tools and workflows on
Kubernetes. It uses cwltool to interpret CWL and creates task pods while managing
workflow dependencies and aggregate resource budgets.

Choose a documentation path for your task:

| Section | Purpose | Start here |
|---|---|---|
| [Tutorials](tutorials/index.md) | Learn by running a small workflow | CWL v1.2 reverse and sort |
| [How-to guides](how-to/index.md) | Complete a specific deployment or execution task | Storage, security, GPU, Dask, provenance |
| [Reference](reference/index.md) | Look up options and supported behavior | Configuration and CWL support |
| [Explanation](explanation/index.md) | Understand architecture and design choices | C4 model, scheduling, storage boundaries |

For production, provide shared persistent storage usable by the runner and task
pods, namespace-scoped runner permissions, and cluster policy appropriate to the
workload. The single-node Minikube tutorial is a development environment, not a
multi-node storage or security certification.

Existing [installation](installation.md), [cluster configuration](cluster-configuration.md),
and [recorded conformance results](conformance/latest-results.md) remain available.
