#!/usr/bin/env python3
"""Validate real Dask computation, sidecar resource limits, and cluster disposal."""
import json
from pathlib import Path
import re
import subprocess
import time

KUBECTL = ['kubectl', '--context', 'calrissian-conformance', '--request-timeout=15s']
NAMESPACE = ['-n', 'calrissian-conformance']


def read_json(*args):
    return json.loads(subprocess.check_output(KUBECTL + list(args) + ['-o', 'json'], text=True))


def main():
    subprocess.run(KUBECTL + NAMESPACE + ['delete', 'job', 'dask-cleanup-smoke', '--ignore-not-found'], check=True)
    subprocess.run(KUBECTL + NAMESPACE + ['delete', 'pods', '-l', 'app=dask-cleanup-smoke', '--ignore-not-found'], check=True)
    # Skaffold installs an immutable local tag. A literal :latest would make
    # the Dask init container pull from a registry instead of using that image.
    subprocess.run(KUBECTL + NAMESPACE + [
        'exec', 'deployment/calrissian-session', '--', 'python', '-c',
        'import shutil; shutil.rmtree("/output/dask-cleanup-smoke", ignore_errors=True)'], check=True)
    deployment = read_json(*NAMESPACE, 'get', 'deployment', 'calrissian-session')
    image = deployment['spec']['template']['spec']['containers'][0]['image']
    manifest = read_json(*NAMESPACE, 'create', '--dry-run=client', '--validate=false',
                         '-f', str(Path(__file__).with_name('smoke-job.yaml')))
    runner = manifest['spec']['template']['spec']['containers'][0]
    runner['image'] = image
    runner['args'][runner['args'].index('--default-container') + 1] = image
    subprocess.run(KUBECTL + ['create', '-f', '-'], input=json.dumps(manifest), text=True, check=True)
    deadline = time.monotonic() + 600
    observed = None
    observed_clusters = set()
    while time.monotonic() < deadline:
        pods = read_json(*NAMESPACE, 'get', 'pods', '-l', 'app=dask-cleanup-smoke')['items']
        if pods:
            observed = pods[0]
            sidecar = next(c for c in observed['spec']['containers'] if c['name'] == 'sidecar-container')
            assert sidecar['resources'] == {
                'requests': {'cpu': '10m', 'memory': '64Mi'},
                'limits': {'cpu': '100m', 'memory': '256Mi'},
            }, sidecar['resources']
        clusters = read_json('-n', 'dask-gateway', 'get', 'daskclusters')['items']
        running = [c for c in clusters if c.get('status', {}).get('phase') == 'Running']
        if running:
            observed_clusters.update(c['metadata']['name'] for c in running)
        job = read_json(*NAMESPACE, 'get', 'job', 'dask-cleanup-smoke')
        if job['status'].get('failed'):
            subprocess.run(KUBECTL + NAMESPACE + ['logs', 'job/dask-cleanup-smoke', '-c', 'runner'])
            raise RuntimeError('Smoke Job failed')
        if job['status'].get('succeeded'):
            break
        time.sleep(1)
    else:
        raise TimeoutError('Smoke Job did not complete in 600 seconds')
    assert observed is not None, 'No task pod observed'
    assert observed_clusters, 'No Running Gateway cluster observed'
    logs = subprocess.check_output(KUBECTL + NAMESPACE + [
        'exec', 'deployment/calrissian-session', '--', 'cat',
        '/output/dask-cleanup-smoke/logs/cleanup-smoke.cwl.log'], text=True)
    assert 'Dask cluster shut down successfully' in logs, logs
    connected = re.search(r'Connected to Dask cluster: dask-gateway\.([a-zA-Z0-9-]+)', logs)
    assert connected, 'No cleanup cluster identity recorded'
    observed_cluster = connected[1]
    assert observed_cluster in observed_clusters, 'Cleanup did not target an observed Running cluster'
    peak = re.search(r'cleanup peak RSS KiB: (\d+)', logs)
    assert peak, 'Cleanup peak RSS was not recorded'
    # The controller reconciles shutdown asynchronously.
    for _ in range(60):
        clusters = read_json('-n', 'dask-gateway', 'get', 'daskclusters')['items']
        remaining = [c for c in clusters if c['metadata']['name'] == observed_cluster]
        if not remaining or remaining[0].get('status', {}).get('phase') == 'Stopped':
            break
        time.sleep(1)
    else:
        raise AssertionError('Gateway cluster remained active after cleanup')
    result = subprocess.check_output(KUBECTL + NAMESPACE + [
        'exec', 'deployment/calrissian-session', '--', 'cat',
        '/output/dask-cleanup-smoke/results/result.txt'], text=True)
    assert result.strip() == 'distributed result: 6', result
    print(json.dumps({
        'job': 'dask-cleanup-smoke', 'cluster': observed_cluster,
        'sidecarResources': sidecar['resources'], 'cleanupPeakRssKiB': int(peak[1]),
        'computation': result.strip(), 'cleanup': 'cluster stopped',
    }, indent=2))


if __name__ == '__main__':
    main()
