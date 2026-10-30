#!/usr/bin/env python3
"""Scheduler-only GPU toleration simulation; no GPU hardware or kubelet is used."""
import argparse
import json
import time
from types import SimpleNamespace
import uuid

from kubernetes import client, config

from calrissian_dask.dask import KubernetesDaskPodBuilder
from calrissian.job import KubernetesPodBuilder
from calrissian.scheduling import load_gpu_tolerations
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True, help='Disposable test cluster with node/namespace administration access')
    args = parser.parse_args()
    config.load_kube_config(context=args.context)
    api = client.CoreV1Api()
    token = 'calrissian-gpu-test-' + uuid.uuid4().hex[:8]
    label = {'calrissian-test/gpu-node': token}
    tolerations = load_gpu_tolerations(Path(__file__).with_name('tolerations.yaml'))
    created_node = created_namespace = False
    try:
        api.create_namespace({'metadata': {'name': token}})
        created_namespace = True
        api.create_node({'apiVersion': 'v1', 'kind': 'Node', 'metadata': {'name': token, 'labels': label},
                         'spec': {'taints': [{'key': 'nvidia.com/gpu', 'effect': 'NoSchedule'}]}})
        created_node = True
        def ready():
            api.patch_node_status(token, {'status': {
                'capacity': {'cpu': '4', 'memory': '8Gi', 'pods': '20', 'nvidia.com/gpu': '4'},
                'allocatable': {'cpu': '4', 'memory': '8Gi', 'pods': '20', 'nvidia.com/gpu': '4'},
                'conditions': [{'type': 'Ready', 'status': 'True', 'reason': 'SchedulingSimulation',
                                'message': 'Synthetic node: no kubelet and no GPU hardware'}]}})
        ready()
        cases = [('gpu-with-toleration', True, True, False, True),
                 ('gpu-without-toleration', True, False, False, False),
                 ('cpu-with-gpu-config', False, True, False, False),
                 ('dask-gpu-with-toleration', True, True, True, True)]
        for name, gpu, configured, dask, expected in cases:
            requirements = [{'class': 'http://commonwl.org/cwltool#CUDARequirement',
                             'cudaDeviceCountMin': 1, 'cudaDeviceCountMax': 1}] if gpu else []
            builder = SimpleNamespace(cwlVersion='v1.2', resources={'cores': 1, 'ram': 512},
                                      requirements=requirements, hints=[])
            parameters = (name, builder, 'worker:simulation', {'HOME': '/tmp'}, [], [], ['true'],
                          None, None, None, {}, label, label, {}, None,
                          {'gpu_tolerations': tolerations if configured else None})
            pod_builder = (KubernetesDaskPodBuilder('http://unused.invalid', True, *parameters)
                           if dask else KubernetesPodBuilder(*parameters))
            if dask:
                pod_builder.dask_requirement = {}
            pod = pod_builder.build()
            pod['metadata']['name'] = name
            assert ('tolerations' in pod['spec']) == (gpu and configured)
            api.create_namespaced_pod(token, pod)
        deadline = time.monotonic() + 60
        results = {}
        observed = {}
        while time.monotonic() < deadline:
            ready()
            for name, gpu, configured, dask, expected in cases:
                pod = api.read_namespaced_pod(name, token)
                condition = next((c for c in pod.status.conditions or [] if c.type == 'PodScheduled'), None)
                if condition is None:
                    continue
                observed[name] = {'status': condition.status, 'reason': condition.reason, 'message': condition.message}
                scheduled = condition.status == 'True'
                if scheduled != expected:
                    continue
                if not expected and (condition.reason != 'Unschedulable' or
                                     'untolerated taint' not in (condition.message or '')):
                    continue
                results[name] = {'scheduled': scheduled, 'expected': expected,
                                 'node': pod.spec.node_name, 'message': condition.message}
            taints = {(t.key, t.effect) for t in api.read_node(token).spec.taints or []}
            if len(results) == len(cases) and taints == {('nvidia.com/gpu', 'NoSchedule')}:
                print(json.dumps({'simulation': True, 'results': results}, indent=2))
                return
            time.sleep(2)
        raise RuntimeError(f'Scheduling expectations not met: {results}; observed: {observed}')
    finally:
        try:
            if created_namespace:
                api.delete_collection_namespaced_pod(token, grace_period_seconds=0)
        finally:
            try:
                if created_node:
                    api.delete_node(token)
            finally:
                if created_namespace:
                    api.delete_namespace(token)


if __name__ == '__main__':
    main()
