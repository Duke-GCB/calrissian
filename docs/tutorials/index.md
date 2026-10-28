# Tutorials

Follow these in order. They modernize the historical reverse-and-sort example
using CWL v1.2 and the chart-managed runner.

1. [Run your first tool](first-tool.md): prepare a local cluster, stage inputs,
   execute a container tool, and inspect its output.
2. [Run a parallel workflow](parallel-workflow.md): connect tools and scatter
   over files while keeping execution within a resource budget.

You need Docker, Minikube, kubectl, Helm, Skaffold, Task, and Python 3 on the
host. Run host commands from the repository root. Cluster creation defaults to
6 CPUs and 12 GiB RAM; adjust the Task variables to fit your machine.
