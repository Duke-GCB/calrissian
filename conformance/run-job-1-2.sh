#!/bin/sh

set -e

NAMESPACE_NAME="calrissian-conformance"

kubectl create --namespace="$NAMESPACE_NAME" -f ConformanceTestsJob-1.2.yaml
kubectl wait --namespace="$NAMESPACE_NAME" --for=condition=Ready --selector=job-name=conformance-tests-1-2 pods --timeout=5m
sh wait-for-job.sh conformance-tests-1-2 results-v1.2.txt
kubectl cp $NAMESPACE_NAME/inspect-volumes:/output/badges-1.2.0/ badges/1.2.0
