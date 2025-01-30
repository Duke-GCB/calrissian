# this code is responsible for creating a Dask cluster
# it's executed by the CWL runner in the context of the Dask Gateway extension
# this is for the prototyping purposes only
import os
import re
import logging
from dask_gateway import Gateway

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def parse_memory(mem_str):
    units = {
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
        "PB": 1000**5,
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
        "P": 1024**5,
    }

    match = re.match(r"(\d+)([A-Za-z]+)", mem_str)
    if not match:
        raise ValueError(f"Invalid memory format: {mem_str}")

    value, unit = match.groups()

    if unit not in units:
        raise ValueError(f"Unknown unit: {unit}")

    return int(value) * units[unit]

target = os.environ.get("DASK_CLUSTER_NAME_PATH", None)
gateway_url = os.environ.get("DASK_GATEWAY_URL", None)
image = os.environ.get("DASK_GATEWAY_IMAGE", None)
worker_cores = os.environ.get("DASK_GATEWAY_WORKER_CORES", None)
worker_cores_limit = os.environ.get("DASK_GATEWAY_WORKER_CORES_LIMIT", None)
worker_memory = os.environ.get("DASK_GATEWAY_WORKER_MEMORY", None)
max_cores = os.environ.get("DASK_GATEWAY_CLUSTER_MAX_CORES", None)
max_ram = os.environ.get("DASK_GATEWAY_CLUSTER_MAX_RAM", None)

logger.info(f"Creating Dask cluster and saving the name to {target}")

gateway = Gateway(gateway_url)

cluster_options = gateway.cluster_options()

cluster_options['image'] = image
cluster_options['worker_cores'] = float(worker_cores)
cluster_options['worker_cores_limit'] = int(worker_cores_limit)

cluster_options['worker_memory'] = worker_memory
#cluster_options["worker_extra_pod_labels"] = {"group": "dask"}

logger.info(f"Cluster options: {cluster_options}")
logger.info(dir(cluster_options))
cluster = gateway.new_cluster(cluster_options, shutdown_on_close=False)

# resource requirements
#worker_cores = 0.5
#worker_cores_limit = 1 # would come from DaskGateway.Requirement.ResourceRequirement.worker_cores_limit (or worker_cores)
#worker_memory = 2 # would come from DaskGateway.Requirement.ResourceRequirement.worker_memory
logger.info(f"Resource requirements: {worker_cores} cores, {worker_memory}")

# scale cluster
# max_cores = 5 # would come from DaskGateway.Requirement.ResourceRequirement.max_cores
# max_ram = 16  # would come from DaskGateway.Requirement.ResourceRequirement.max_ram
logger.info(f"Resource limits: {max_cores} cores, {max_ram} GB RAM")

workers = int(min(int(max_cores) // int(worker_cores_limit), parse_memory(max_ram) // parse_memory(worker_memory)))

logger.info(f"Scaling cluster to {workers} workers")
cluster.scale(workers)


# save the cluster name to a file
with open(target, "w") as f:
    f.write(cluster.name)
logger.info(f"Cluster name {cluster.name} saved to {target}")