#!/bin/sh

set -e

NAMESPACE_NAME="calrissian-conformance"

kubectl create --namespace="$NAMESPACE_NAME" -f ConformanceTestsJob-1.1.yaml
kubectl wait --namespace="$NAMESPACE_NAME" --for=condition=Ready --selector=job-name=conformance-tests-1-1 pods
kubectl logs --namespace="$NAMESPACE_NAME" -f jobs/conformance-tests-1-1 | egrep -v 'The `label`' > results-v1.1.txt
kubectl cp $NAMESPACE_NAME/inspect-volumes:/output/badges-1.1.0/ badges/1.1.0
