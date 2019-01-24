class: Workflow
doc: "Reverse the lines in a document, then sort those lines."
cwlVersion: v1.0

inputs:
  input:
    type: File
    doc: "The input file to be processed."
  reverse_sort:
    type: boolean
    default: true
    doc: "If true, reverse (decending) sort"
outputs:
  output:
    type: File
    outputSource: sorted/output
    doc: "The output with the lines reversed and sorted."
steps:
  rev:
    in:
      input: input
    out: [output]
    run: revtool-no-docker.cwl

  sorted:
    in:
      input: rev/output
      reverse: reverse_sort
    out: [output]
    run: sorttool-no-docker.cwl
