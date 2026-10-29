# Label task pods

Create a YAML **map** named `pod-labels.yaml` in the runner:

```yaml
app.kubernetes.io/part-of: example-workflow
example.org/run: demo-001
```

Run with the file mounted/readable in the runner. This command assumes the shared
staging paths and workflow identity described in the [storage guide](storage.md):

```sh
calrissian --max-ram 4G --max-cores 2 \
  --pod-serviceaccount calrissian-workflow \
  --tmpdir-prefix /shared/tmp/ --tmp-outdir-prefix /shared/tmpout/ \
  --outdir /shared/results --pod-labels /config/pod-labels.yaml \
  workflow.cwl inputs.yml
```

The labels apply to spawned task Pods, not automatically to the runner Job.
Set runner labels separately in its Kubernetes manifest. Avoid overriding keys
used by admission or network policy; network-access label configuration can
add or replace keys when the generated task labels are assembled.

If configuration is managed outside the runner image, create a ConfigMap:

```sh
kubectl -n YOUR_NAMESPACE create configmap task-labels --from-file=pod-labels.yaml
```

Mount it in the runner at `/config`, using the following fragments in the
appropriate container and pod sections of its manifest:

```yaml
# runner container volumeMounts
volumeMounts:
  - name: task-labels
    mountPath: /config
    readOnly: true
# pod spec volumes
volumes:
  - name: task-labels
    configMap:
      name: task-labels
```

To inspect retained task pods:

```sh
kubectl -n YOUR_NAMESPACE get pods -l example.org/run=demo-001 --show-labels
```

Completed tasks are normally deleted. Set runner `CALRISSIAN_DELETE_PODS=false`
when you need to inspect them after execution, and delete retained pods after
review. Labels alone do not enforce [network isolation](security.md).
