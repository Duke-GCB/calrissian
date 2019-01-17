class: Workflow
doc: "Run failtool which will fail"
cwlVersion: v1.0

inputs:
  input:
    type: File
    doc: "The input file to be processed."
outputs:
  output:
    type: File
    outputSource: fail/output
    doc: "The output"
steps:
  fail:
    in:
      input: input
    out: [output]
    run: failtool.cwl
