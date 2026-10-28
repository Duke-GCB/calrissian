cwlVersion: v1.2
class: CommandLineTool
requirements:
  DockerRequirement:
    dockerPull: debian:bookworm-slim
  ResourceRequirement:
    coresMin: 1
    ramMin: 64
baseCommand: rev
inputs:
  source:
    type: File
    inputBinding:
      position: 1
stdout: $(inputs.source.basename).reversed.txt
outputs:
  reversed:
    type: File
    outputBinding:
      glob: $(inputs.source.basename).reversed.txt
