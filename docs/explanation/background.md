# Why Calrissian uses Kubernetes

Calrissian combines cwltool's CWL interpretation with a Kubernetes task backend.
Workflow authors describe tools, input/output types, dependencies, and resource
requirements in CWL. Cluster operators provide the infrastructure on which
those descriptions can execute: nodes, images, storage, identities, and policy.

This separation makes the same workflow description reusable across suitably
configured clusters. It does not guarantee that every image, filesystem, CWL
extension, or cloud service works unchanged. The [support reference](../reference/cwl-support.md)
and recorded conformance results describe current coverage and limitations.

The runner creates task Pods rather than invoking a local Docker daemon.
Kubernetes handles node placement; Calrissian handles the workflow graph and
aggregate resource reservations.
Cluster autoscaling, if enabled, is an
operator-provided facility: Calrissian does not directly scale node pools.

API retries and watch recovery do not imply automatic retries of failed tool
commands or recreation of task pods removed during disruption.

See the [C4 architecture](architecture.md) for the system boundaries and
[execution explanation](execution.md) for the scheduling model. This replaces
the historical site's broad claims about automatic scaling, retries, and full
CWL portability with the behavior implemented by the current runner.
