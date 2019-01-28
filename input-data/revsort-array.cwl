class: Workflow
doc: "Reverse the lines in a document, then sort those lines."
cwlVersion: v1.0

requirements:
  - class: ScatterFeatureRequirement

inputs:
  input:
    type: File[]
    doc: "Array of input files to be processed."
  reverse_sort:
    type: boolean
    default: true
    doc: "If true, reverse (descending) sort"
outputs:
  output:
    type: File[]
    outputSource: sorted/output
    doc: "The output files with the lines reversed and sorted."
steps:
  rev:
    in:
      input: input
    out: [output]
    scatter: input
    run: revtool.cwl
    requirements:
      - class: ResourceRequirement
        coresMin: 1
        ramMin: 100
  sorted:
    in:
      input: rev/output
      reverse: reverse_sort
    scatter: input
    out: [output]
    run: sorttool.cwl
    requirements:
      - class: ResourceRequirement
        coresMin: 1
        ramMin: 100
