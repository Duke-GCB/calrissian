#!/bin/sh

set -e

NAMESPACE_NAME="calrissian-conformance"

kubectl create --namespace="$NAMESPACE_NAME" -f ConformanceTestsJob-1.0.yaml
kubectl wait --namespace="$NAMESPACE_NAME" --for=condition=Ready --selector=job-name=conformance-tests-1-0 pods
kubectl logs --namespace="$NAMESPACE_NAME" -f jobs/conformance-tests-1-0 | egrep -v 'The `label`' > results-v1.1.txt
kubectl cp $NAMESPACE_NAME/inspect-volumes:/output/badges-1.0.2/ badges/1.0.2
