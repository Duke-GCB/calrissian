# Project diagrams

These diagrams are generated from the current Python source using Pyreverse.
For the system context, containers, and components, see the
[C4 architecture diagrams](../explanation/architecture.md).

## Classes

[Open the class diagram](../uml/classes.svg).

![Calrissian classes and inheritance](../uml/classes.svg)

## Packages

[Open the package diagram](../uml/packages.svg).

![Calrissian package dependencies](../uml/packages.svg)

## Generate locally

Install Task, uv, and Graphviz (`dot`), then run from the repository root:

```sh
task docs:generate
```

This generates the CLI table, Python API reference, and UML diagrams. To generate
only one, use `task docs:cli`, `task docs:api`, or `task docs:uml`. Generated output under
`docs/api/` and `docs/uml/` is ignored by Git.

`task docs:serve` generates these before starting the documentation preview.
