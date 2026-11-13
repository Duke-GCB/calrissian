#!/usr/bin/env python3
"""Run extension CWL cases and independently verify their Kubernetes lifecycle."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET

from version_config import load_version_config, validate_deployed_versions

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('cleanup', ROOT / 'test-cleanup.py')
cleanup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup)
K = cleanup.KUBECTL
NS = cleanup.NAMESPACE
read_json = cleanup.read_json


def command(*args, **kwargs):
    kwargs.setdefault('timeout', 150)
    return subprocess.check_output(K + list(args), text=True, **kwargs)


def remote_file(path):
    return command(*NS, 'exec', 'deployment/calrissian-session', '--', 'cat', path)


def deployed_package_version(package):
    expression = (
        'from importlib.metadata import version; '
        f'print(version({package!r}))'
    )
    return command(
        *NS, 'exec', 'deployment/calrissian-session', '--', 'python', '-c', expression
    ).strip()


def deployed_gateway_chart_version():
    context = cleanup.KUBECTL[cleanup.KUBECTL.index('--context') + 1]
    releases = subprocess.check_output([
        'helm', '--kube-context', context, '--namespace', 'dask-gateway',
        'list', '--output', 'json',
    ], text=True)
    release = next(
        (item for item in json.loads(releases) if item.get('name') == 'dask-gateway'),
        None,
    )
    chart = release.get('chart', '') if release else ''
    prefix = 'dask-gateway-'
    if not chart.startswith(prefix):
        raise RuntimeError(f"Could not determine deployed Dask Gateway chart version: {chart!r}")
    return chart.removeprefix(prefix)


def stopped(name):
    clusters = read_json('-n', 'dask-gateway', 'get', 'daskclusters')['items']
    terminated = all(c.get('status', {}).get('phase') == 'Stopped'
                     for c in clusters if c['metadata']['name'] == name)
    pods = read_json('-n', 'dask-gateway', 'get', 'pods', '-l',
                     f'gateway.dask.org/cluster={name}')['items']
    return terminated and not pods


def run_case(case, image, directory, timeout):
    name = f'dask-test-{case["id"]}-{uuid.uuid4().hex[:8]}'
    output = f'/output/{name}'
    identities = set()
    observed_pods = set()
    result = dict(case, job=name, cwl='failed', lifecycle='failed', clusters_observed=[])
    created = False
    try:
        cm = read_json(*NS, 'get', 'configmap', 'dask-cleanup-smoke')
        data = cm['data']
        data.update({p.name: p.read_text() for p in (ROOT / 'tests').iterdir()
                     if p.suffix in ('.cwl', '.py')})
        data['pod-labels.yaml'] = json.dumps({'dask-suite-run': name})
        command(*NS, 'create', '-f', '-', input=json.dumps({
            'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': name}, 'data': data}))
        created = True
        manifest = read_json(*NS, 'create', '--dry-run=client', '--validate=false', '-f', str(ROOT / 'smoke-job.yaml'))
        manifest['metadata']['name'] = name
        manifest['spec']['activeDeadlineSeconds'] = timeout
        pod = manifest['spec']['template']['spec']
        runner = pod['containers'][0]
        runner['image'] = image
        args = runner['args']
        for flag, value in [('--default-container', image), ('--dask-script-configmap', name),
                            ('--tool-logs-basepath', output + '/logs'), ('--outdir', output + '/results')]:
            args[args.index(flag) + 1] = value
        args[-1] = '/smoke/' + case['tool']
        for volume in pod['volumes']:
            if volume['name'] == 'smoke':
                volume['configMap']['name'] = name
        command(*NS, 'create', '-f', '-', input=json.dumps(manifest))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            pods = read_json(*NS, 'get', 'pods', '-l', f'dask-suite-run={name}')['items']
            for task in pods:
                observed_pods.add(task['metadata']['name'])
                if task['spec'].get('serviceAccountName') != 'calrissian-workflow':
                    raise AssertionError('Task did not use the unprivileged workflow ServiceAccount')
                sidecar = next((c for c in task['spec']['containers'] if c['name'] == 'sidecar-container'), None)
                if not case['clusters']:
                    if sidecar is not None:
                        raise AssertionError('Ordinary CWL task unexpectedly received a Dask sidecar')
                    continue
                if sidecar is None:
                    raise AssertionError('Dask task is missing its cleanup sidecar')
                expected = {'requests': {'cpu': '10m', 'memory': '64Mi'}, 'limits': {'cpu': '100m', 'memory': '256Mi'}}
                if sidecar['resources'] != expected:
                    raise AssertionError(f'Unexpected sidecar resources: {sidecar["resources"]}')
                for init in task.get('status', {}).get('initContainerStatuses', []):
                    if 'terminated' in init.get('state', {}):
                        try:
                            logs = command(*NS, 'logs', task['metadata']['name'], '-c', init['name'],
                                           stderr=subprocess.STDOUT)
                        except subprocess.CalledProcessError as exc:
                            # The runtime deletes finished task pods concurrently.
                            if '(NotFound)' in exc.output:
                                continue
                            raise
                        identities.update(re.findall(r'Cluster name dask-gateway\.([\w-]+) saved', logs))
            job = read_json(*NS, 'get', 'job', name)
            if job.get('status', {}).get('succeeded') or job.get('status', {}).get('failed'):
                break
            time.sleep(1)
        else:
            raise TimeoutError(f'{name} exceeded {timeout}s')
        logs = command(*NS, 'logs', 'job/' + name, '-c', 'runner')
        (directory / f'{name}.log').write_text(logs)
        success = bool(job.get('status', {}).get('succeeded'))
        try:
            if success == case['should_fail']:
                raise AssertionError('Unexpected CWL exit status; see runner log')
            if not case['should_fail']:
                actual = remote_file(output + '/results/result.txt').strip()
                if actual != 'distributed result: 6':
                    raise AssertionError(f'Unexpected result: {actual!r}')
            elif not re.search(case.get('failure_pattern', 'permanentFail'), logs):
                raise AssertionError('Expected CWL failure diagnostic was not reported')
            result['cwl'] = 'passed'
        except Exception as exc:
            result['error'] = f'{type(exc).__name__}: {exc}'
        # Runtime task logs survive deletion of task pods. Use exact cluster identities.
        script = ('import pathlib; p=pathlib.Path(' + repr(output + '/logs') + '); '
                  'print("\\n".join(f.read_text() for f in p.rglob("*.log")))')
        tool_logs = command(*NS, 'exec', 'deployment/calrissian-session', '--', 'python', '-c', script)
        (directory / f'{name}-tools.log').write_text(tool_logs)
        if case.get('failure_marker') and case['failure_marker'] not in tool_logs:
            result['cwl'] = 'failed'
            result['error'] = 'The intended command failure was not reached'
        cleaned = set(re.findall(r'Connected to Dask cluster: dask-gateway\.([\w-]+)', tool_logs))
        identities.update(cleaned)
        if len(cleaned) != case['clusters'] or len(identities) != case['clusters']:
            raise AssertionError(f'Expected {case["clusters"]} clusters, observed {sorted(identities)}, cleaned {sorted(cleaned)}')
        if case['clusters'] and (not observed_pods or 'Dask cluster shut down successfully' not in tool_logs):
            raise AssertionError('Missing task observation or successful cleanup log')
        deadline = time.monotonic() + 60
        while not all(stopped(n) for n in identities):
            if time.monotonic() >= deadline:
                raise TimeoutError('Owned Gateway clusters remained active after completion')
            time.sleep(1)
        result['lifecycle'] = 'passed'
    except (Exception, KeyboardInterrupt) as exc:
        result['error'] = f'{type(exc).__name__}: {exc}'
        result['interrupted'] = isinstance(exc, KeyboardInterrupt)
    finally:
        if created and (result['cwl'] != 'passed' or result['lifecycle'] != 'passed'):
            # Save diagnostic state before deleting a failed or interrupted Job.
            try:
                # An init container may have provisioned a cluster since the
                # last poll. Let its log stream finish before removing the pod.
                tasks = read_json(*NS, 'get', 'pods', '-l', f'dask-suite-run={name}')['items']
                for task in tasks:
                    for init in task.get('status', {}).get('initContainerStatuses', []):
                        if not any(s in init.get('state', {}) for s in ('running', 'terminated')):
                            continue
                        try:
                            init_logs = command(*NS, 'logs', task['metadata']['name'], '-c',
                                                init['name'], '--follow', stderr=subprocess.STDOUT,
                                                timeout=90)
                        except subprocess.TimeoutExpired as exc:
                            init_logs = exc.output or ''
                            if isinstance(init_logs, bytes):
                                init_logs = init_logs.decode(errors='replace')
                        except subprocess.CalledProcessError as exc:
                            if '(NotFound)' in exc.output:
                                continue
                            raise
                        identities.update(re.findall(r'Cluster name dask-gateway\.([\w-]+) saved', init_logs))
                        (directory / f'{name}-{init["name"]}.log').write_text(init_logs)
                if not (directory / f'{name}.log').exists():
                    logs = command(*NS, 'logs', 'job/' + name, '-c', 'runner', '--pod-running-timeout=5s')
                    (directory / f'{name}.log').write_text(logs)
                state = command(*NS, 'get', 'pods', '-l', f'dask-suite-run={name}', '-o', 'yaml')
                (directory / f'{name}-pods.yaml').write_text(state)
            except Exception as exc:
                result['diagnostic_error'] = str(exc)
        result['clusters_observed'] = sorted(identities)
        # On cancellation or failure, only stop clusters identified in this run's logs.
        try:
            if created:
                command(*NS, 'delete', 'job', name, '--ignore-not-found', '--wait=true', '--timeout=30s')
                command(*NS, 'delete', 'pods', '-l', f'dask-suite-run={name}', '--ignore-not-found', '--wait=true', '--timeout=30s')
            for cluster in identities:
                if not stopped(cluster):
                    code = ('from dask_gateway import Gateway; gateway=Gateway("http://traefik-dask-gateway.dask-gateway"); '
                            'cluster=gateway.connect(' + repr('dask-gateway.' + cluster) + '); '
                            'cluster.shutdown(); gateway.close()')
                    command(*NS, 'exec', 'deployment/calrissian-session', '--', 'python', '-c', code)
            if created:
                command(*NS, 'delete', 'configmap', name, '--ignore-not-found')
        except Exception as exc:
            result['lifecycle'] = 'failed'
            result['cleanup_error'] = str(exc)
    return result


def main():
    def interrupt(signum, frame):
        raise KeyboardInterrupt(f'Received signal {signum}')
    signal.signal(signal.SIGTERM, interrupt)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--id', type=int)
    parser.add_argument('--timeout', type=int, default=600)
    args = parser.parse_args()
    cases = json.loads((ROOT / 'tests/suite.json').read_text())
    if args.id is not None:
        cases = [c for c in cases if c['id'] == args.id]
        if not cases:
            parser.error('Unknown test ID')
    if args.timeout < 30:
        parser.error('--timeout must be at least 30 seconds')
    command(*NS, 'rollout', 'status', 'deployment/calrissian-session', '--timeout=120s')
    command('-n', 'dask-gateway', 'wait', '--for=condition=Available', 'deployment', '--all', '--timeout=120s')
    deployment = read_json(*NS, 'get', 'deployment', 'calrissian-session')
    image = deployment['spec']['template']['spec']['containers'][0]['image']
    config = load_version_config()
    dask_version = deployed_package_version('dask')
    distributed_version = deployed_package_version('distributed')
    gateway_client_version = deployed_package_version('dask-gateway')
    gateway_chart_version = deployed_gateway_chart_version()
    actual_versions, errors = validate_deployed_versions(
        config, dask_version, distributed_version,
        gateway_client_version, gateway_chart_version,
    )
    expected_version = os.environ.get('DASK_VERSION')
    if expected_version and dask_version != expected_version:
        errors.append(
            f"Deployed Dask {dask_version} does not match selected DASK_VERSION {expected_version}"
        )
    print(f"Deployed versions: {json.dumps(actual_versions, sort_keys=True)}", flush=True)
    if errors:
        print('Version contract failed:', file=sys.stderr)
        for error in errors:
            print(f'- {error}', file=sys.stderr)
        print('Deploy the configured versions with `task dask:deploy`.', file=sys.stderr)
        return 1

    directory = ROOT / 'test-results' / ('suite-' + uuid.uuid4().hex[:8])
    directory.mkdir(parents=True)
    (directory / 'suite.json').write_text(json.dumps({
        **actual_versions,
        'cases': cases,
    }, indent=2) + '\n')
    results = []
    suite = ET.Element('testsuite', name=f'dask-gateway-extension-{dask_version}')
    try:
        for case in cases:
            print(f'Running {case["id"]}: {case["name"]}', flush=True)
            result = run_case(case, image, directory, args.timeout)
            results.append(result)
            print(json.dumps(result), flush=True)
            element = ET.SubElement(suite, 'testcase', name=f'{case["id"]}-{case["name"]}')
            if result['cwl'] != 'passed' or result['lifecycle'] != 'passed':
                ET.SubElement(element, 'failure').text = json.dumps(result)
            if result.get('interrupted'):
                break
    finally:
        (directory / 'results.json').write_text(json.dumps({
            'image': image,
            **actual_versions,
            'results': results,
        }, indent=2) + '\n')
        suite.set('tests', str(len(results)))
        suite.set('failures', str(len(suite.findall('testcase/failure'))))
        ET.ElementTree(suite).write(directory / 'junit.xml', encoding='unicode')
        print(f'Results: {directory}', flush=True)
    return int(any(r['cwl'] != 'passed' or r['lifecycle'] != 'passed' for r in results))


if __name__ == '__main__':
    raise SystemExit(main())
