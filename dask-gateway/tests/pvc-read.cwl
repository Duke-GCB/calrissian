cwlVersion: v1.2
class: CommandLineTool
$namespaces:
  calrissian: https://calrissian-cwl.github.io/schema#
label: Read a staged PVC file from a Dask worker
requirements:
  DockerRequirement:
    dockerPull: calrissian-pvc-probe:cohort
  ResourceRequirement:
    coresMin: 1
    ramMin: 512
  NetworkAccess:
    networkAccess: true
  calrissian:DaskGatewayRequirement:
    workerCores: 1
    workerCoresLimit: 1
    workerMemory: 1G
    clusterMaxCores: 1
    clusterMaxMemory: 1G
baseCommand: [python, '-c']
arguments:
  - position: 0
    valueFrom: |
      import errno
      import json
      import os
      import sys
      from pathlib import Path
      from dask_gateway import Gateway
      from distributed import get_worker
      def read_on_worker(path, directory):
          target = Path(directory) / "worker-write.txt"
          result = {"worker": get_worker().address, "path": path, "worker_output": str(target),
                    "text": None, "read_error": None, "output_write_error": None, "input_write_errno": None}
          try:
              target.write_text("Worker output is writable.\n")
          except OSError as error:
              result["output_write_error"] = str(error)
          try:
              result["text"] = Path(path).read_text()
          except OSError as error:
              result["read_error"] = str(error)
          try:
              with Path(path).open("r+"):
                  pass
          except OSError as error:
              result["input_write_errno"] = error.errno
          return result
      with Gateway(os.environ["DASK_GATEWAY_URL"]) as gateway:
          with gateway.connect(os.environ["DASK_CLUSTER"], shutdown_on_close=False) as cluster:
              with cluster.get_client() as client:
                  client.wait_for_workers(1, timeout=120)
                  result = client.submit(read_on_worker, sys.argv[1], os.getcwd(), pure=False).result(timeout=120)
                  assert result["read_error"] is None, result
                  assert result["output_write_error"] is None, result
                  assert result["input_write_errno"] == errno.EROFS, result
                  result["input_read_only"] = True
                  assert result["text"] == "Read this file from the execution PVC.\n"
                  target = Path(result["worker_output"])
                  assert target.read_text() == "Worker output is writable.\n"
                  target.unlink()
                  result["cluster"] = cluster.name
                  print(json.dumps(result))
inputs:
  source:
    type: File
    inputBinding:
      position: 1
stdout: result.json
outputs:
  result:
    type: File
    outputBinding:
      glob: result.json
