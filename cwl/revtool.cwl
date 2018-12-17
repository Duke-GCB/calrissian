class: CommandLineTool
cwlVersion: v1.0
doc: "Reverse each line using the `rev` command"
requirements:
  - class: DockerRequirement
    dockerPull: debian:stretch-slim
inputs:
  input:
    type: File
    inputBinding: {}
outputs:
  output:
    type: File
    outputBinding:
      glob: output.txt
baseCommand: rev
stdout: output.txt
