# Upgrade a deployment

Upgrade with a reproducible image and a small verification run before moving
normal workflows. Start with the [compatibility scope](../reference/compatibility.md)
and the release/source changes between your current and target versions.

## Capture the current deployment

Record the image digest, dependency versions, manifests, workflow configuration,
RBAC, storage configuration, and effective network policies. Archive completed
outputs and reports. Keep the previous image available for rollback.

Finish active runs or follow [cancellation and cleanup](cleanup.md) before
replacing their runner. An upgrade does not migrate a running task or resume a
partially completed workflow.

## Review behavior that affects existing deployments

| Setting | Current behavior | Migration action |
|---|---|---|
| Runner/task identities | Runner has management RBAC; tasks select a separate account | Follow the [identity migration](../cluster-configuration.md#migrate-an-existing-installation); remove obsolete default-account grants only after checking other users. |
| Task API tokens | Automatic token mount is disabled by default | Opt in only for tools that need API credentials and use an explicitly authorized task account. |
| Container filesystem | Read-only root filesystem with writable mounted output/scratch | Test tools that write into their image directories; use a declared output mount or a deliberate override. |
| NetworkAccess | Policies and labels must agree | Recheck both allowed and denied traffic after changing chart values or CNI. |
| JSON configuration | Only the first `--conf` file is loaded | Consolidate defaults into one file; see [precedence](../reference/configuration.md#configuration-precedence). |
| Dask cleanup resources | Independent runner-controlled requests/limits | Preserve explicit overrides and rerun the cleanup integration test. |

These are current behaviors to review, not a claim that all changed in the same
release. Compare the relevant tags to determine which affect your upgrade.

## Build and verify the candidate

Build a fresh runner image from the target source and push an immutable tag or
digest. Rebuild derived Dask images with compatible client/worker versions.
Update your Job template or deployment values, rather than modifying a running
Job's immutable Pod template. The conformance chart is a local integration
harness; its values are not a universal production chart contract.

In an isolated namespace using the intended storage and policies:

1. Run the [standalone smoke tool](deployment.md) and retrieve its outputs.
2. Run a representative multi-step/scatter workflow with your images and inputs.
3. Verify task identity, filesystem permissions, network access, and resource requests.
4. Retrieve usage reports and provenance if your service depends on them.
5. For Dask/GPU users, run the relevant integration checks separately.

## Roll out or roll back

Submit new runs using the verified image and configuration. Watch runner and
task failures and compare artifacts with the baseline. A container image update
alone does not update external runner Jobs, RBAC, storage, or network policies.

To roll back, stop new submissions, preserve candidate-run evidence, and restore
the previous image and matching configuration for new Jobs. Use fresh run/output
paths. Restoring an image does not undo external tool side effects or recover
lost computation; review partial runs before repeating them.
