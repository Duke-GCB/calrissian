{
  "cwlVersion": "v1.2",
  "class": "Workflow",
  "inputs": {},
  "outputs": {
    "result": {
      "type": "File",
      "outputSource": "first/result"
    }
  },
  "steps": {
    "first": {
      "run": "minimal.cwl",
      "in": {},
      "out": [
        "result"
      ]
    }
  }
}
