# Submit and monitor a workflow

For an interactive local run, use the [tutorial's chart-managed runner](../tutorials/first-tool.md).
For a service, batch system, or Argo workflow, submit a Kubernetes Job containing
the Calrissian runner, its PVC mounts, and the runner ServiceAccount. Set
`restartPolicy: Never` and choose `backoffLimit` explicitly; `0` avoids automatic
whole-workflow retries. Keep the Job name for subsequent monitoring.

Configure task identity independently with `--pod-serviceaccount`. The runner
needs Kubernetes management permissions; tasks normally do not. Follow the
[identity examples](../cluster-configuration.md) rather than binding permissions
to the namespace's default account.

Watch the submitted Job's status conditions for **both** Complete and Failed,
and inspect runner termination details and logs. Waiting only for Complete can
leave clients waiting until their timeout after a failure. Calrissian returns
the workflow exit status; outputs and reports should be on persistent storage.

```sh
kubectl -n YOUR_NAMESPACE get job YOUR_JOB -o yaml
kubectl -n YOUR_NAMESPACE logs -f job/YOUR_JOB
```

Replace both placeholders with your deployment values. In Argo, a resource
template submitting a Kubernetes Job keeps the runner lifecycle explicit and
avoids depending on container ordering inside an Argo step pod. Calrissian runs
tool commands in task containers, not another cwl-runner in every task.

Raw task pods are not automatically recreated after node disruption. Watch
reconnection recovers monitoring, not lost computation; choose a workflow-level
retry policy that accounts for side effects and partial outputs.
