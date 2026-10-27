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
      "run": "command-failure.cwl",
      "in": {},
      "out": [
        "result"
      ]
    }
  }
}
