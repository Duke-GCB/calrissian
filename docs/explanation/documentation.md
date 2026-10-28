# Documentation structure and maintenance

The site follows Diátaxis: tutorials teach through a worked example, how-tos
solve specific tasks, reference supplies exact options, and explanation supplies
mental models and tradeoffs.

| Existing material | New location / role |
|---|---|
| Historical examples/README reverse-sort walkthrough | CWL v1.2 tutorials with runnable fixtures |
| installation.md | Installation how-to, retained URL |
| cluster-configuration.md | Detailed operator how-to, retained URL |
| Versioned conformance pages | Reference navigation |
| Former docs/comformance diagnostics | Consolidated full-suite results and limitations under docs/conformance |
| Dask setup and suite README | Linked from Dask how-to |
| Missing generated API/class pages | Removed from navigation; source links remain available |

Conformance evidence is consolidated under `docs/conformance/`: one complete-run
summary, version pages, and cross-suite limitations replace focused-test reports.
Other existing guide URLs are preserved. New pages direct readers to the
existing detailed cluster guide rather than duplicating complete policy/RBAC
manifests. Future editorial work can split that guide while retaining compatibility
pages. Historical repository examples remain available but are not the current
security/deployment tutorial.

All diagrams use checked-in `.puml` sources and `.svg` renderings embedded in
Markdown. To regenerate the three C4 views, install Java and a PlantUML JAR with
its bundled C4 standard library, then run from the repository root:

```sh
java -jar "$PLANTUML_JAR" -tsvg docs/explanation/diagrams/*.puml
```

The sources use Smetana layout to avoid a separate Graphviz dependency and use
bundled includes rather than remote URLs. Commit source and rendering together.
Build the site with `hatch run docs:mkdocs build`. Keep generated site output
outside the documentation source tree when reviewing changes.

Conformance badges render the conformance tool's JSON output from
`conformance/badges/` on the `develop` branch through Badgen. After a complete
run, commit the collected JSON and update the versioned result tables and badge
overview when categories or results change.

Remaining work includes expanding production cloud/storage walkthroughs with
verified environments, evolving the retained operator guide, and maintaining
release-specific evidence. This bootstrap does not imply those deployments have
been validated. The tutorial fixtures and documented commands should be checked
when updating cwltool, images, or the chart.

## Adaptation of the companion documentation site

The [companion documentation repository](https://github.com/calrissian-cwl/calrissian-cwl.github.io)
provided additional background, environment, priority, security, labels,
selectors, logs, and experimental Dask material. These topics are adapted here
against the current implementation, rather than copied verbatim:

- Background and scheduling distinguish reservations from measured utilization,
  and distinguish runner queues from Kubernetes Pending pods.
- GPU quantities describe devices, not GPU cores; selectors follow CUDA
  requirement/hint detection while tolerations follow actual positive allocation.
- Logs describe captured Kubernetes streams and the actual per-job file layout.
- Security distinguishes pod-level identity from container-level restrictions.
- Dask examples use valid YAML and describe cleanup as dependent on the task and
  infrastructure lifecycle, not an unconditional guarantee.

The companion site's water-bodies Kubernetes Job tutorial is intentionally not
imported. The two local CWL v1.2 tutorials remain the guided introduction.
Publishing destination consolidation remains an editorial/deployment decision;
this branch changes only the current repository.

## Writing style

Use short paragraphs, each covering one idea. Prefer bullets for prerequisites,
configuration choices, checks, and limitations. Use numbered steps for a required
sequence and tables for option mappings.

Keep setup instructions separate from test-controller internals. Link directly
to a focused reference section when extra detail is needed. Preserve important
qualifications when shortening text, especially validation scope and historical
results.
