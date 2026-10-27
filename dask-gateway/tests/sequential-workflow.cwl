{
  "cwlVersion": "v1.2",
  "class": "Workflow",
  "inputs": {},
  "outputs": {
    "result": {
      "type": "File",
      "outputSource": "second/result"
    }
  },
  "steps": {
    "first": {
      "run": "minimal.cwl",
      "in": {},
      "out": [
        "result"
      ]
    },
    "second": {
      "run": {
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
            "clusterMaxCores": 1,
            "clusterMaxMemory": "256M"
          }
        },
        "inputs": {
          "previous": "File"
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
      },
      "in": {
        "previous": "first/result"
      },
      "out": [
        "result"
      ]
    }
  }
}
