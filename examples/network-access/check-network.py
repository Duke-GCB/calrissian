#!/usr/bin/env python3
"""Validate policy enforcement using real CWL tasks and an in-cluster HTTP control."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

import yaml


ROOT = Path(__file__).resolve().parent
LABEL = 'calrissian.example/network-access'
RUNNER = """
import os, pathlib, signal, subprocess, sys
pidfile = pathlib.Path(sys.argv[1])
child = subprocess.Popen(sys.argv[2:], start_new_session=True)
pidfile.write_text(str(child.pid))
try:
    try:
        result = child.wait(timeout=90)
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait()
        raise
finally:
    pidfile.unlink(missing_ok=True)
sys.exit(result)
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', default='calrissian-conformance')
    parser.add_argument('--namespace', default='calrissian-conformance')
    args = parser.parse_args()
    kube = ['kubectl', '--context', args.context, '--request-timeout=15s', '-n', args.namespace]
    def command(*arguments, **kwargs):
        return subprocess.check_output(kube + list(arguments), text=True, timeout=150, **kwargs)
    name = 'network-check-' + uuid.uuid4().hex[:8]
    runner_path = '/tmp/' + name
    output = '/output/' + name
    created = []
    process = None
    pid_path = None
    results = []
    def create(kind, resource):
        command('create', '-f', '-', input=json.dumps(resource))
        created.append((kind, resource['metadata']['name']))
    try:
        session = json.loads(command('get', 'deployment', 'calrissian-session', '-o', 'json'))
        image = session['spec']['template']['spec']['containers'][0]['image']
        create('serviceaccount', {'apiVersion': 'v1', 'kind': 'ServiceAccount',
               'metadata': {'name': name}, 'automountServiceAccountToken': False})
        allowed, denied = {LABEL: name + '-allowed'}, {LABEL: name + '-denied'}
        for policy, labels in zip(yaml.safe_load_all((ROOT / 'network-policies.yaml').read_text()), (denied, allowed)):
            policy['metadata']['name'] = next(iter(labels.values()))
            policy['spec']['podSelector']['matchLabels'] = labels
            create('networkpolicy', policy)
        selector = {'calrissian-network-check/endpoint': name}
        create('service', {'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': name},
                          'spec': {'selector': selector, 'ports': [{'port': 8080, 'targetPort': 8080}]}})
        service = json.loads(command('get', 'service', name, '-o', 'json'))
        address = service['spec']['clusterIP']
        ipv6 = ':' in address
        url = 'http://' + ('[' + address + ']' if ipv6 else address) + ':8080/'
        create('deployment', {'apiVersion': 'apps/v1', 'kind': 'Deployment', 'metadata': {'name': name},
               'spec': {'replicas': 1, 'selector': {'matchLabels': selector}, 'template': {
                   'metadata': {'labels': selector}, 'spec': {'automountServiceAccountToken': False,
                       'containers': [{'name': 'http', 'image': image, 'imagePullPolicy': 'IfNotPresent',
                           'command': ['python', '-u', '-m', 'http.server', '8080', '--bind',
                                       '::' if ipv6 else '0.0.0.0', '--directory', '/tmp'],
                           'readinessProbe': {'httpGet': {'path': '/', 'port': 8080}, 'periodSeconds': 1}}]}}}})
        command('rollout', 'status', 'deployment/' + name, '--timeout=120s')
        files = {p.name: p.read_text() for p in ROOT.glob('*.cwl')}
        files.update({'allowed-labels.yaml': yaml.safe_dump(allowed), 'denied-labels.yaml': yaml.safe_dump(denied),
                      'pod-labels.yaml': json.dumps({'calrissian-network-check/run': name}),
                      'network-config.json': json.dumps({'network_access_pod_labels': runner_path + '/allowed-labels.yaml',
                                                        'no_network_access_pod_labels': runner_path + '/denied-labels.yaml'})})
        for case in ('allowed-before', 'denied', 'allowed-after'):
            files[case + '-pod-labels.yaml'] = json.dumps({'calrissian-network-check/run': name, 'calrissian-network-check/case': case})
        stage = ('import json,pathlib,sys; root=pathlib.Path(' + repr(runner_path) + '); '
                 'root.mkdir(); [ (root / k).write_text(v) for k,v in json.load(sys.stdin).items()]')
        command('exec', '-i', 'deployment/calrissian-session', '--', 'python', '-c', stage, input=json.dumps(files))
        for case, enabled in [('allowed-before', True), ('denied', False), ('allowed-after', True)]:
            print('Running ' + case, flush=True)
            case_output = output + '/' + case
            arguments = ['calrissian', '--max-ram', '1G', '--max-cores', '1', '--default-container', image,
                '--pod-serviceaccount', name, '--pod-labels', runner_path + '/' + case + '-pod-labels.yaml',
                '--conf', runner_path + '/network-config.json',
                '--outdir', case_output, runner_path + ('/allowed.cwl' if enabled else '/denied.cwl'), '--url', url]
            observed = False
            with tempfile.TemporaryFile(mode='w+') as logs:
                pid_path = runner_path + '/' + case + '.pid'
                process = subprocess.Popen(kube + ['exec', 'deployment/calrissian-session', '--', 'python', '-c', RUNNER, pid_path] + arguments,
                                           stdout=logs, stderr=subprocess.STDOUT, text=True)
                deadline = time.monotonic() + 120
                while process.poll() is None:
                    pods = json.loads(command('get', 'pods', '-l', 'calrissian-network-check/run=' + name + ',calrissian-network-check/case=' + case, '-o', 'json'))['items']
                    for pod in pods:
                        observed = True
                        expected = allowed if enabled else denied
                        assert pod['metadata']['labels'].get(LABEL) == expected[LABEL], pod['metadata']
                        assert pod['spec']['serviceAccountName'] == name
                    if time.monotonic() >= deadline:
                        raise TimeoutError(case)
                    time.sleep(0.5)
                logs.seek(0)
                captured = logs.read()
                status = process.returncode
                process = None
                assert observed, 'No task pod observed: ' + captured
                assert (status == 0) == enabled, captured
                raw = command('exec', 'deployment/calrissian-session', '--', 'cat', case_output + '/result.json')
                result = json.loads(raw)
                assert result['url'] == url and result['connected'] is enabled, result
                if enabled:
                    assert result['status'] == 200, result
                else:
                    assert 'error' in result and 'permanentFail' in captured, captured
                results.append({'case': case, **result})
        print(json.dumps({'passed': True, 'results': results, 'output': output}, indent=2))
    finally:
        if process is not None:
            # Stop the remote CLI before removing pods, avoiding creation races.
            for sig in (15, 9):
                stop = ('import os,pathlib; p=pathlib.Path(' + repr(pid_path) + '); '
                        'pid=int(p.read_text()) if p.exists() else None; '
                        'cmd=pathlib.Path("/proc/"+str(pid)+"/cmdline"); '
                        'owned=cmd.exists() and ' + repr(name.encode()) + ' in cmd.read_bytes(); '
                        'os.killpg(pid,' + str(sig) + ') if owned else None')
                command('exec', 'deployment/calrissian-session', '--', 'python', '-c', stop)
                try:
                    process.wait(timeout=10)
                    break
                except subprocess.TimeoutExpired:
                    continue
            if process.poll() is None:
                process.kill()
                process.wait()
        # Only task pods carrying this run's unique label are removed.
        command('delete', 'pods', '-l', 'calrissian-network-check/run=' + name,
                '--ignore-not-found', '--wait=true', '--timeout=30s')
        for kind, resource_name in reversed(created):
            command('delete', kind, resource_name, '--ignore-not-found', '--wait=true', '--timeout=30s')
        cleanup = 'import shutil; shutil.rmtree(' + repr(runner_path) + ', ignore_errors=True)'
        command('exec', 'deployment/calrissian-session', '--', 'python', '-c', cleanup)


if __name__ == '__main__':
    main()
