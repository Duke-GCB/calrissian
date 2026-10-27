import os
import time
from dask_gateway import Gateway

# Keep the task alive long enough for the external test to inspect the sidecar.
time.sleep(15)
gateway = Gateway(os.environ["DASK_GATEWAY_URL"])
cluster = gateway.connect(os.environ["DASK_CLUSTER"])
client = cluster.get_client()
client.wait_for_workers(1, timeout=120)
assert client.submit(sum, [1, 2, 3]).result(timeout=60) == 6
print("distributed result: 6", flush=True)
client.close()
# Cluster disposal is deliberately left to the cleanup sidecar.
