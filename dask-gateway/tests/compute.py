"""Exercise actual workers; cluster disposal belongs to Calrissian's sidecar."""
import os
import sys
import time
from dask_gateway import Gateway

time.sleep(5)  # Allow the host harness to observe the task and its init logs.
gateway = Gateway(os.environ['DASK_GATEWAY_URL'])
cluster = gateway.connect(os.environ['DASK_CLUSTER'])
client = cluster.get_client()
expected = min(int(os.environ['DASK_GATEWAY_CLUSTER_MAX_CORES']) //
               int(os.environ['DASK_GATEWAY_WORKER_CORES_LIMIT']),
               {'256M': 1, '512M': 2, '768M': 3}[os.environ['DASK_GATEWAY_CLUSTER_MAX_RAM']])
client.wait_for_workers(expected, timeout=120)
assert len(client.scheduler_info()['workers']) == expected
assert client.submit(sum, [1, 2, 3]).result(timeout=60) == 6
print('distributed result: 6', flush=True)
client.close()
if '--fail' in sys.argv:
    print('DASK_SUITE_EXPECTED_EXIT_7', file=sys.stderr, flush=True)
    sys.exit(7)
