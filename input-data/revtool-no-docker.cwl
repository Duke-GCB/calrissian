class: CommandLineTool
cwlVersion: v1.0
doc: "Reverse each line using the `rev` command"
requirements:
  - class: InlineJavascriptRequirement
inputs:
  input:
    type: File
    inputBinding: {}
outputs:
  output:
    type: File
    outputBinding:
      glob: $('reversed-' + inputs.input.basename)
baseCommand: rev
stdout: $('reversed-' + inputs.input.basename)
