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
    },
    "calrissian:DaskGatewayRequirement": {
      "workerCores": 1,
      "workerCoresLimit": 1,
      "workerMemory": "256M",
      "clusterMaxCores": 2,
      "clusterMaxMemory": "512M"
    }
  },
  "inputs": {},
  "baseCommand": [
    "python",
    "/controller/compute.py"
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
