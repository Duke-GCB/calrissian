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
      "workerCores": "$(inputs.workerCores)",
      "workerCoresLimit": "$(inputs.workerCoresLimit)",
      "workerMemory": "$(inputs.workerMemory)",
      "clusterMaxCores": "$(inputs.clusterMaxCores)",
      "clusterMaxMemory": "$(inputs.clusterMaxMemory)"
    }
  },
  "inputs": {
    "workerCores": {
      "type": "int",
      "default": 1
    },
    "workerCoresLimit": {
      "type": "int",
      "default": 1
    },
    "workerMemory": {
      "type": "string",
      "default": "256M"
    },
    "clusterMaxCores": {
      "type": "int",
      "default": 1
    },
    "clusterMaxMemory": {
      "type": "string",
      "default": "256M"
    }
  },
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
