#!/bin/sh

set -eu

namespace="${NAMESPACE_NAME:-calrissian-conformance}"
job_name="$1"
result_file="$2"
deadline=$(( $(date +%s) + 7200 ))

while :; do
  succeeded=$(kubectl get --namespace="$namespace" job "$job_name" -o jsonpath='{.status.succeeded}')
  failed=$(kubectl get --namespace="$namespace" job "$job_name" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}')

  if [ "${succeeded:-0}" -gt 0 ]; then
    kubectl logs --namespace="$namespace" "job/$job_name" | sed '/The `label`/d' > "$result_file"
    exit 0
  fi

  if [ "${failed:-}" = True ] || [ "$(date +%s)" -ge "$deadline" ]; then
    kubectl logs --namespace="$namespace" "job/$job_name" > "$result_file" || true
    kubectl describe --namespace="$namespace" job "$job_name" >> "$result_file" || true
    exit 1
  fi

  sleep 10
done
