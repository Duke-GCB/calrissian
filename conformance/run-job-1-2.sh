#!/bin/sh

set -e

NAMESPACE_NAME="calrissian-conformance"

kubectl create --namespace="$NAMESPACE_NAME" -f ConformanceTestsJob-1.2.yaml
kubectl wait --namespace="$NAMESPACE_NAME" --for=condition=Ready --selector=job-name=conformance-tests-1-2 pods
kubectl logs --namespace="$NAMESPACE_NAME" -f jobs/conformance-tests-1-2 > results-1-2.txt