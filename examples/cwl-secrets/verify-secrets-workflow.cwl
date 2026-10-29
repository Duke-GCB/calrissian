#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: Workflow
$namespaces:
  cwltool: http://commonwl.org/cwltool#

hints:
  cwltool:Secrets:
    secrets: [env_secret, argument_secret, binding_secret, staged_secret, writable_staged_secret]

inputs:
  env_secret: string
  argument_secret: string
  binding_secret: string
  staged_secret: string
  writable_staged_secret: string

outputs:
  result:
    type: File
    outputSource: verify/result
  diagnostics:
    type: File
    outputSource: verify/diagnostics

steps:
  verify:
    in:
      env_secret: env_secret
      argument_secret: argument_secret
      binding_secret: binding_secret
      staged_secret: staged_secret
      writable_staged_secret: writable_staged_secret
    out: [result, diagnostics]
    run: verify-secrets.cwl
