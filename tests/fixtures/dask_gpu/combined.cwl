cwlVersion: v1.2
class: CommandLineTool
$namespaces:
  cwltool: http://commonwl.org/cwltool#
  calrissian: https://calrissian-cwl.github.io/schema#
baseCommand: ["true"]
inputs: []
outputs: []
requirements:
  DockerRequirement:
    dockerPull: ubuntu:24.04
  cwltool:CUDARequirement:
    cudaVersionMin: "11.0"
    cudaComputeCapability: ["7.0"]
    cudaDeviceCountMin: 1
    cudaDeviceCountMax: 1
  calrissian:DaskGatewayRequirement:
    workerCores: 1
    workerCoresLimit: 1
    workerMemory: "1G"
    clusterMaxCores: 1
    clusterMaxMemory: "1G"
