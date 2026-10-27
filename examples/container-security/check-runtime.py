#!/usr/bin/env python3
"""Check generated security contexts with strict admission and real containers."""
import argparse
import json
import subprocess
import time
from types import SimpleNamespace
import uuid

from calrissian.context import CalrissianRuntimeContext
from calrissian.job import CalrissianCommandLineJob, KubernetesPodBuilder
from calrissian.dask import KubernetesDaskPodBuilder

PROBE = r'''
import errno, json, os
from pathlib import Path
expected_read_only = READ_ONLY
path = Path('/app/calrissian-security-probe')
try:
    path.write_text('probe')
except OSError as exc:
    assert expected_read_only and exc.errno == errno.EROFS, (expected_read_only, exc)
else:
    assert not expected_read_only, 'root filesystem was writable'
    path.unlink()
for path in (Path('/work/volume-probe'), Path('/tmp/temp-probe')):
    path.write_text('writable')
    path.unlink()
status = Path('/proc/self/status').read_text()
assert any(line.split() == ['NoNewPrivs:', '1'] for line in status.splitlines())
print(json.dumps({'readOnlyRootFilesystem': expected_read_only, 'NoNewPrivs': 1,
                  'writableOutputAndTmp': True}))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', default='calrissian-conformance')
    args = parser.parse_args()
    kube = ['kubectl', '--context', args.context, '--request-timeout=15s', '-n', 'calrissian-conformance']
    def command(*arguments, **kwargs):
        return subprocess.check_output(kube + list(arguments), text=True, timeout=60, **kwargs)
    deployment = json.loads(command('get', 'deployment', 'calrissian-session', '-o', 'json'))
    image = deployment['spec']['template']['spec']['containers'][0]['image']
    results = []
    for read_only in (True, False):
        runtime = CalrissianRuntimeContext({'no_read_only': not read_only})
        job = object.__new__(CalrissianCommandLineJob)
        builder = SimpleNamespace(cwlVersion='v1.2', resources={'cores': 0.1, 'ram': 128}, requirements=[], hints=[])
        volumes = [{'name': 'work', 'emptyDir': {}}, {'name': 'tmp', 'emptyDir': {}}]
        mounts = [{'name': 'work', 'mountPath': '/work'}, {'name': 'tmp', 'mountPath': '/tmp'}]
        # Match the integration image's UID/GID; fsGroup makes the test emptyDir writable.
        pod_context = {'runAsUser': 1001, 'runAsGroup': 0, 'fsGroup': 0}
        parameters = ('security', builder, image, {'HOME': '/work'}, mounts, volumes,
                      ['true'], 'nested/result.json', None, None, {}, {}, {}, pod_context,
                      None, job.get_pod_additional_spec(runtime))
        pod = KubernetesPodBuilder(*parameters).build()
        dask = KubernetesDaskPodBuilder('http://unused.invalid', True, *parameters)
        dask.dask_requirement = {}
        dask_pod = dask.build()
        # Strict validation also checks both Dask init-container names are unique.
        for manifest in (pod, dask_pod):
            manifest['metadata']['name'] = 'security-dry-run-' + uuid.uuid4().hex[:8]
            command('create', '--dry-run=server', '--validate=strict', '-f', '-',
                    input=json.dumps(manifest))
        name = 'security-runtime-' + uuid.uuid4().hex[:8]
        pod['metadata']['name'] = name
        code = PROBE.replace('READ_ONLY', repr(read_only))
        # Keep stdout redirection through the real builder, exercising nested mkdir init.
        import shlex
        pod['spec']['containers'][0]['args'] = ['python -c ' + shlex.quote(code) +
                                               ' > nested/result.json && cat nested/result.json']
        created = False
        try:
            command('create', '-f', '-', input=json.dumps(pod))
            created = True
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                actual = json.loads(command('get', 'pod', name, '-o', 'json'))
                expected = job.get_container_security_context(runtime)
                for container in actual['spec']['containers'] + actual['spec']['initContainers']:
                    assert container['securityContext'] == expected, container
                phase = actual.get('status', {}).get('phase')
                if phase in ('Succeeded', 'Failed'):
                    logs = command('logs', name, '-c', pod['spec']['containers'][0]['name'])
                    assert phase == 'Succeeded', logs
                    results.append(json.loads(logs.strip()))
                    break
                time.sleep(1)
            else:
                raise TimeoutError(name)
        finally:
            if created:
                command('delete', 'pod', name, '--ignore-not-found', '--wait=true', '--timeout=30s')
    print(json.dumps({'image': image, 'strictAdmission': 'ordinary and Dask passed', 'results': results}, indent=2))


if __name__ == '__main__':
    main()
