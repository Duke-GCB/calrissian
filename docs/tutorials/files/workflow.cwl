cwlVersion: v1.2
class: Workflow
requirements:
  ScatterFeatureRequirement: {}
inputs:
  sources: File[]
steps:
  reverse:
    run: reverse.cwl
    in:
      source: sources
    scatter: source
    out: [reversed]
  sort:
    run: sort.cwl
    in:
      source: reverse/reversed
    scatter: source
    out: [sorted]
outputs:
  sorted:
    type: File[]
    outputSource: sort/sorted
