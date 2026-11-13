# Calrissian extension packages

Calrissian discovers optional integration packages through the
`calrissian.extensions` Python entry-point group. Core should provide the
generic extension host; integration-specific behavior belongs in its own
distribution.

## Register an extension

Declare an entry point in the extension package's `pyproject.toml`:

```toml
[project.entry-points."calrissian.extensions"]
example = "example_calrissian_extension:create_extension"
```

The factory returns an object with `api_version = 1` and a required
`register_arguments(parser)` method. The host also calls these optional hooks
when they are defined:

| Hook | Purpose |
|---|---|
| `configure_runtime(args, runtime_context)` | Store extension settings in `runtime_context.extension_options` |
| `custom_schema_callback(args)` | Return a zero-argument callback to register process schemas, or `None` |
| `make_job_runner(tool, runtime_context)` | Return a runner class for a supported process, or `None` |
| `cleanup(args, runtime_context)` | Clean up extension resources; return true if it handled pod cleanup |
| `logger_names` | Iterable of logger names whose level follows Calrissian verbosity |

The extension host calls hooks in entry-point discovery order, except cleanup,
which runs in reverse order. A `make_job_runner` hook returning `None` lets the
next extension try; the first non-`None` runner is selected.

## Package the extension as a unit

Keep the runner integration and its runtime resources in the extension
distribution. The base `calrissian` package should not import that package
directly. For the Dask Gateway extension, the separate `calrissian-dask`
distribution contains the job integration, CWL schema, and lifecycle scripts.
The runner reads its bundled scripts and embeds them in task commands by
default; the existing ConfigMap override remains available.

Task images provide `dask-gateway` because they execute the lifecycle scripts.
They do not need to install `calrissian-dask`.
