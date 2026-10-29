#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: CommandLineTool
$namespaces:
  cwltool: http://commonwl.org/cwltool#

hints:
  "cwltool:Secrets":
    secrets: [env_secret, argument_secret, binding_secret, staged_secret, writable_staged_secret]
  EnvVarRequirement:
    envDef:
      - envName: CWL_SECRET_ENV
        envValue: $(inputs.env_secret)
  InitialWorkDirRequirement:
    listing:
      - entryname: verify-secrets.sh
        entry: |
          #!/bin/sh
          set -eu
          # Leave time for the runner's log follower to attach before exiting.
          sleep 2
          check_matches_fixture() {
            label=$1
            value=$2
            expected_hash=$3
            if ! printf '%s' "$value" | sha256sum | grep -q "$expected_hash"; then
              echo "$label was not substituted correctly" >&2
              exit 1
            fi
          }
          check_matches_fixture environment "$CWL_SECRET_ENV" 46c3ed3b215edfbffe70644053ca571e3bf159e441b3c24d663b7e0213ee6894
          check_matches_fixture argument "$1" 614d70bca6595e9edc97058c89099afef5ab081095817c12e0b71f78b91c5d9d
          check_matches_fixture input-binding "$2" af1696d5b3273e9b505cca65e74240ae88eccabe785b1ff1f8f8d4feeeb6c8a8
          check_matches_fixture argument-and-staged-input "$3" 61044a044b44105c9cea6efe6b2271cf6fb7082877fa02a6a983d537a15755f6
          if ! IFS= read -r staged_secret < staged-secret.txt && test -z "$staged_secret"; then
            echo 'staged secret file could not be read' >&2
            exit 1
          fi
          check_matches_fixture staged-file "$staged_secret" 61044a044b44105c9cea6efe6b2271cf6fb7082877fa02a6a983d537a15755f6
          if ! IFS= read -r writable_staged_secret < writable-staged-secret.txt && test -z "$writable_staged_secret"; then
            echo 'writable staged secret file could not be read' >&2
            exit 1
          fi
          check_matches_fixture writable-staged-file "$writable_staged_secret" 82b6e7e7d47ed323424b6c780404a787efd34d1d6b99f563aa17dc75002830a1
          printf 'CWL secret substitution checks passed\n'
      - entryname: staged-secret.txt
        entry: $(inputs.staged_secret)
      - entryname: writable-staged-secret.txt
        entry: $(inputs.writable_staged_secret)
        writable: true

inputs:
  env_secret: string
  argument_secret: string
  binding_secret:
    type: string
    inputBinding:
      position: 3
  staged_secret: string
  writable_staged_secret: string

outputs:
  result:
    type: stdout
  diagnostics:
    type: File
    outputBinding:
      glob: verification.err

baseCommand: [sh]
arguments:
  - position: 1
    valueFrom: verify-secrets.sh
  - position: 2
    valueFrom: $(inputs.argument_secret)
  - position: 4
    valueFrom: $(inputs.staged_secret)
stdout: verification.txt
stderr: verification.err
