# Storage and security boundaries

## Runner and task identities

The runner is a trusted orchestrator with Kubernetes management permissions.
Workflow task code is a separate trust boundary: task Pods should use a distinct
ServiceAccount with automatic token mounting disabled. This reduces accidental
credential exposure; it does not sandbox all untrusted workload behavior.

## Shared storage

PVC-backed files cross the runner/task boundary. Writable staged copies must be
reachable by both processes; task-local emptyDir scratch cannot be used as a
cross-pod handoff. A symlink resolves in the filesystem namespace of its reader,
so different mount paths need careful mapping. Output materialization and
collection must preserve content even when intermediate references differ.

## Filesystem restrictions

A read-only root filesystem applies to the container image filesystem, while
explicit output and temporary mounts remain writable. UID/GID matching can
help filesystem access but cannot override storage-driver limitations or cluster
admission policy. Disabling user matching lets those deployment defaults apply.

## Network policy

Network labels express intent; installed and enforced policy controls traffic.
Policies are additive, and all containers in a Pod share its network namespace.
Review CNI behavior and other matching policies before claiming isolation.

## Retained provenance

Provenance is an additional retained data surface. CWLProv/BagIt bundles can
include input content and logs; review access and publication policy alongside
ordinary outputs. The format is separate from RO-Crate compliance and from
resource-usage accounting.
