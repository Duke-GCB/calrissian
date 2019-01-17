class: CommandLineTool
doc: "Fail example"
cwlVersion: v1.0
requirements:
  - class: DockerRequirement
    dockerPull: debian:stretch-slim
  - class: InlineJavascriptRequirement
inputs:
  - id: input
    type: File

outputs:
  - id: output
    type: File
    outputBinding:
      glob: $('failed-' + inputs.input.basename)

baseCommand: ['ls','/foo']
stdout: $('failed-' + inputs.input.basename)
