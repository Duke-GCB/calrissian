#!/bin/sh

set -e

kubectl create -f ConformanceTestsJob.yaml
kubectl wait --for=condition=Ready --selector=job-name=conformance-tests pods
kubectl logs -f jobs/conformance-tests
