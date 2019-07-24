#!/bin/bash

kubectl run --attach=true conformance-tests --overrides='
{
  "spec": {
    "containers": [
      {
        "name": "conformance-tests",
        "image": "calrissian:conformance",
        "workingDir": "/conformance/common-workflow-language-1.0.2",
        "command": ["./run_test.sh"],
        "args": ["--junit-xml=/output/calrissian-conformance.xml", "--classname=calrissian", "RUNNER=calrissian", "-n1", "--verbose", "EXTRA=--max-ram 2G --max-cores 4 --default-container debian:stretch-slim"],
        "stdin": true,
        "stdinOnce": true,
        "tty": true,
        "env": [
          { "name":"CALRISSIAN_POD_NAME", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } },
          { "name": "TMPDIR", "value": "/output" }
        ],
        "volumeMounts": [
          { "mountPath": "/conformance", "name": "conformance-test-data", "readOnly": true },
          { "mountPath": "/output", "name": "conformance-output-data" }
        ]
      }
    ],
    "restartPolicy": "Never",
    "volumes": [
      { "name": "conformance-test-data", "persistentVolumeClaim": {"claimName":"conformance-test-data", "readOnly": true } },
      { "name": "conformance-output-data", "persistentVolumeClaim": {"claimName": "conformance-output-data" } }
    ]
  }
}
'  --image=calrissian:conformance --restart=Never
