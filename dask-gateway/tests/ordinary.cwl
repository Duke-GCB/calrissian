{
  "cwlVersion": "v1.2",
  "class": "CommandLineTool",
  "$namespaces": {
    "calrissian": "https://calrissian-cwl.github.io/schema#"
  },
  "requirements": {
    "ResourceRequirement": {
      "coresMin": 1,
      "ramMin": 512
    }
  },
  "inputs": {},
  "baseCommand": [
    "python",
    "-c",
    "print(\"distributed result: 6\")"
  ],
  "stdout": "result.txt",
  "outputs": {
    "result": {
      "type": "File",
      "outputBinding": {
        "glob": "result.txt"
      }
    }
  }
}
