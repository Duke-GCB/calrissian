import time, os, sys
import logging
from dask_gateway import Gateway

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise RuntimeError(f"Required environment variable '{name}' is not set")
    return value

source = require_env("DASK_CLUSTER_NAME_PATH")
gateway_url = require_env("DASK_GATEWAY_URL")
signal = "/shared/completed"

logger.info(f"Sidecar: Waiting for completion signal ({signal}) from main container...")

# Poll for the existence of the completion file
while not os.path.exists(signal):
    logger.info("Sidecar: Waiting for completion signal from main container...")
    time.sleep(5)

logger.info("Sidecar: Completion signal received. Shutting down Dask cluster...")

# Shut down the Dask cluster
with open(source, "r") as f:
    cluster_name = f.read().strip()

gateway = Gateway(gateway_url)
cluster = gateway.connect(cluster_name)
logger.info(f"Sidecar: Connected to Dask cluster: {cluster_name}")
cluster.shutdown()
logger.info("Sidecar: Dask cluster shut down successfully.")
sys.exit(0)