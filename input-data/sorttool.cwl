class: CommandLineTool
doc: "Sort lines using the `sort` command"
cwlVersion: v1.0
requirements:
  - class: DockerRequirement
    dockerPull: debian:stretch-slim
  - class: InlineJavascriptRequirement
inputs:
  - id: reverse
    type: boolean
    inputBinding:
      position: 1
      prefix: "--reverse"
  - id: input
    type: File
    inputBinding:
      position: 2

outputs:
  - id: output
    type: File
    outputBinding:
      glob: $('sorted-' + inputs.input.basename)

baseCommand: sort
stdout: $('sorted-' + inputs.input.basename)
