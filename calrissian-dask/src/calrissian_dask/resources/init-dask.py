# this code is responsible for creating a Dask cluster
# it's executed by the CWL runner in the context of the Dask Gateway extension
# this is for the prototyping purposes only
import os
import re
import logging
from dask_gateway import Gateway

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def parse_memory(mem_str: str) -> int:
    """
    Parse a memory size string and return its value in bytes.

    Supported formats:
      - Decimal units: KB, MB, GB, TB, PB (base 1000)
      - Binary units:  K,  M,  G,  T,  P  (base 1024)

    :param mem_str: Memory size as a string (e.g. "2GB", "512M")
    :return: Size in bytes
    """
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

    if not isinstance(mem_str, str) or not mem_str:
        raise ValueError("Memory value must be a non-empty string")

    match = re.fullmatch(r"(\d+)\s*([A-Za-z]+)", mem_str.strip())
    if not match:
        raise ValueError(
            f"Invalid memory format '{mem_str}'. "
            "Expected format like '512MB', '2G', or '1024K'."
        )

    value, unit = match.groups()
    unit = unit.upper()

    if unit not in units:
        raise ValueError(
            f"Unknown memory unit '{unit}'. "
            f"Supported units are: {', '.join(sorted(units.keys()))}."
        )

    return int(value) * units[unit]


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise RuntimeError(f"Required environment variable '{name}' is not set")
    return value


target = require_env("DASK_CLUSTER_NAME_PATH")
gateway_url = require_env("DASK_GATEWAY_URL")
image = require_env("DASK_GATEWAY_IMAGE")
worker_cores = require_env("DASK_GATEWAY_WORKER_CORES")
worker_cores_limit = require_env("DASK_GATEWAY_WORKER_CORES_LIMIT")
worker_memory = require_env("DASK_GATEWAY_WORKER_MEMORY")
max_cores = require_env("DASK_GATEWAY_CLUSTER_MAX_CORES")
max_ram = require_env("DASK_GATEWAY_CLUSTER_MAX_RAM")

logger.info(f"Creating Dask cluster and saving the name to {target}")

gateway = Gateway(gateway_url)

cluster_options = gateway.cluster_options()

cluster_options['image'] = image
cluster_options['worker_cores'] = float(worker_cores)
cluster_options['worker_cores_limit'] = int(worker_cores_limit)

cluster_options['worker_memory'] = worker_memory

logger.info(f"Cluster options: {cluster_options}")
logger.info(dir(cluster_options))
cluster = gateway.new_cluster(cluster_options, shutdown_on_close=False)
# Resource requirements:
# The values for worker_cores, worker_cores_limit, and worker_memory are expected
# to be provided by DaskGateway.Requirement.ResourceRequirement (for example,
# via worker_cores_limit or worker_cores, and worker_memory).
logger.info(f"Resource requirements: {worker_cores} cores, {worker_memory}")
# Cluster-wide limits:
# The values for max_cores and max_ram are also expected to come from
# DaskGateway.Requirement.ResourceRequirement.max_cores and
# DaskGateway.Requirement.ResourceRequirement.max_ram, respectively.

worker_cores_limit = int(worker_cores_limit)
worker_mem = parse_memory(worker_memory)

if worker_cores_limit <= 0:
    raise ValueError("workerCoresLimit must be > 0")
if worker_mem <= 0:
    raise ValueError("workerMemory must be > 0")

workers = min(
    int(max_cores) // worker_cores_limit,
    parse_memory(max_ram) // worker_mem,
)

logger.info(f"Scaling cluster to {workers} workers")
cluster.scale(workers)


# save the cluster name to a file
with open(target, "w") as f:
    f.write(cluster.name)
logger.info(f"Cluster name {cluster.name} saved to {target}")