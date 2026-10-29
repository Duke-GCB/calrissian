cwlVersion: v1.2
class: CommandLineTool
requirements:
  DockerRequirement:
    dockerPull: debian:bookworm-slim
  ResourceRequirement:
    coresMin: 1
    ramMin: 64
baseCommand: sort
inputs:
  source:
    type: File
    inputBinding:
      position: 1
stdout: $(inputs.source.basename).sorted.txt
outputs:
  sorted:
    type: File
    outputBinding:
      glob: $(inputs.source.basename).sorted.txt
