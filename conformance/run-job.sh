#!/bin/sh

set -e

kubectl create -f ConformanceTestsJob-1.1.yaml
kubectl wait --for=condition=Ready --selector=job-name=conformance-tests-1-1 pods
kubectl logs -f jobs/conformance-tests-1-1
