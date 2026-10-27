#!/usr/bin/env python3
"""Verify task token mounts without reading or printing credential contents."""
import argparse
import json
import shlex
import subprocess
import time
from types import SimpleNamespace
import uuid

from calrissian.context import CalrissianRuntimeContext
from calrissian.job import CalrissianCommandLineJob, KubernetesPodBuilder
from calrissian.dask import KubernetesDaskPodBuilder

TOKEN_DIR = '/var/run/secrets/kubernetes.io/serviceaccount'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', default='calrissian-conformance')
    args = parser.parse_args()
    kube = ['kubectl', '--context', args.context, '--request-timeout=15s', '-n', 'calrissian-conformance']
    def command(*arguments, **kwargs):
        return subprocess.check_output(kube + list(arguments), text=True, timeout=60, **kwargs)
    deployment = json.loads(command('get', 'deployment', 'calrissian-session', '-o', 'json'))
    image = deployment['spec']['template']['spec']['containers'][0]['image']
    name = 'task-token-test-' + uuid.uuid4().hex[:8]
    created = False
    results = []
    try:
        command('create', '-f', '-', input=json.dumps({'apiVersion': 'v1', 'kind': 'ServiceAccount',
                'metadata': {'name': name}, 'automountServiceAccountToken': True}))
        created = True
        for enabled in (False, True):
            runtime = CalrissianRuntimeContext({'pod_automount_service_account_token': enabled})
            job = object.__new__(CalrissianCommandLineJob)
            builder = SimpleNamespace(cwlVersion='v1.2', resources={'cores': 0.1, 'ram': 128}, requirements=[], hints=[])
            parameters = (name, builder, image, {'HOME': '/tmp'}, [], [], ['true'],
                          None, None, None, {}, {}, {}, {}, name, job.get_pod_additional_spec(runtime))
            for kind in (KubernetesPodBuilder, KubernetesDaskPodBuilder):
                pod_builder = (KubernetesDaskPodBuilder('http://unused.invalid', True, *parameters)
                               if kind is KubernetesDaskPodBuilder else kind(*parameters))
                if kind is KubernetesDaskPodBuilder:
                    pod_builder.dask_requirement = {}
                pod = pod_builder.build()
                pod['metadata']['name'] = name + ('-enabled' if enabled else '-disabled')
                admitted = json.loads(command('create', '--dry-run=server', '--validate=strict', '-f', '-',
                                              '-o', 'json', input=json.dumps(pod)))
                assert admitted['spec']['automountServiceAccountToken'] is enabled
                projected = [v for v in admitted['spec'].get('volumes', []) if any(
                    'serviceAccountToken' in s for s in v.get('projected', {}).get('sources', []))]
                assert bool(projected) == enabled
                for container in admitted['spec']['containers'] + admitted['spec'].get('initContainers', []):
                    automatic_mount = any(m['mountPath'] == TOKEN_DIR for m in container.get('volumeMounts', []))
                    assert automatic_mount == enabled
                if kind is KubernetesDaskPodBuilder:
                    continue  # Real Dask execution is covered by the compatibility suite.
                code = ('import os,json; exists=os.path.isfile(' + repr(TOKEN_DIR + '/token') + '); '
                        f'assert exists is {enabled!r}; print(json.dumps({{"tokenFilePresent":exists}}))')
                pod['spec']['containers'][0]['args'] = ['python -c ' + shlex.quote(code)]
                pod_created = False
                try:
                    command('create', '-f', '-', input=json.dumps(pod))
                    pod_created = True
                    deadline = time.monotonic() + 120
                    while time.monotonic() < deadline:
                        actual = json.loads(command('get', 'pod', pod['metadata']['name'], '-o', 'json'))
                        phase = actual.get('status', {}).get('phase')
                        if phase in ('Succeeded', 'Failed'):
                            logs = command('logs', pod['metadata']['name'], '-c', pod['spec']['containers'][0]['name'])
                            assert phase == 'Succeeded', logs
                            result = json.loads(logs.strip())
                            assert result['tokenFilePresent'] is enabled
                            results.append(result)
                            break
                        time.sleep(1)
                    else:
                        raise TimeoutError(pod['metadata']['name'])
                finally:
                    if pod_created:
                        command('delete', 'pod', pod['metadata']['name'], '--ignore-not-found', '--wait=true', '--timeout=30s')
        sessions = json.loads(command('get', 'pods', '-l', 'app=calrissian-session', '-o', 'json'))['items']
        assert sessions and any(any('serviceAccountToken' in source for source in v.get('projected', {}).get('sources', []))
                                for p in sessions for v in p['spec'].get('volumes', []))
        print(json.dumps({'strictAdmission': 'ordinary and Dask passed', 'runnerTokenMountPreserved': True,
                          'image': image, 'results': results}, indent=2))
    finally:
        if created:
            command('delete', 'serviceaccount', name, '--ignore-not-found')


if __name__ == '__main__':
    main()
