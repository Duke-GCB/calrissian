#!/usr/bin/env python3
"""Run one numbered conformance test using an existing suite Job manifest."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid


def prepare_job(job, version, test_id, suffix):
    name = f"conformance-{version.replace('.', '-')}-{test_id}-{suffix}"
    job['metadata']['name'] = name
    container = job['spec']['template']['spec']['containers'][0]
    args = container['args']
    args[args.index('--'):args.index('--')] = ['-n', str(test_id)]
    badge_path = f'/output/{name}-badges'
    args[args.index('--badgedir') + 1] = badge_path
    return name, badge_path, container['name']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', required=True, choices=['1.0', '1.1', '1.2', '1.2.1'])
    parser.add_argument('--id', required=True, type=int)
    parser.add_argument('--context', default='calrissian-conformance')
    parser.add_argument('--namespace', default='calrissian-conformance')
    parser.add_argument('--dry-run', action='store_true', help='Print the Job without creating it')
    options = parser.parse_args()
    if options.id < 1:
        parser.error('--id must be a positive test number')
    directory = Path(__file__).resolve().parent
    kubectl = ['kubectl', '--context', options.context, '-n', options.namespace]
    manifest = directory / f'ConformanceTestsJob-{options.version}.yaml'
    job = json.loads(subprocess.check_output(kubectl + [
        'create', '--dry-run=client', '--validate=false', '-f', str(manifest), '-o', 'json']))
    name, badges, container = prepare_job(job, options.version, options.id, uuid.uuid4().hex[:8])
    if options.dry_run:
        print(json.dumps(job, indent=2))
        return 0
    subprocess.run(kubectl + ['create', '-f', '-'], input=json.dumps(job), text=True, check=True)
    results = directory / f'{name}.txt'
    print(f'Job: {name}\nLogs: {results}', flush=True)
    try:
        with results.open('w') as output:
            process = subprocess.Popen(kubectl + [
                'logs', '-f', f'job/{name}', '-c', container, '--pod-running-timeout=120s'],
                stdout=subprocess.PIPE, text=True)
            for line in process.stdout:
                output.write(line)
                output.flush()
                print(line, end='', flush=True)
            if process.wait():
                raise RuntimeError(f'Cannot collect logs; inspect job/{name}')
        # The log stream can close just before Kubernetes records termination.
        deadline = time.monotonic() + 60
        exit_code = None
        while time.monotonic() < deadline:
            pods = json.loads(subprocess.check_output(kubectl + [
                'get', 'pods', '-l', f'job-name={name}', '-o', 'json', '--request-timeout=10s']))
            for pod in pods['items']:
                for status in pod['status'].get('containerStatuses', []):
                    if status['name'] == container and 'terminated' in status['state']:
                        exit_code = status['state']['terminated']['exitCode']
            if exit_code is not None:
                break
            time.sleep(2)
        if exit_code is None:
            raise RuntimeError(f'No final container status; inspect job/{name}')
        # Keep single-test badges separate from the full-suite badges.
        destination = directory / 'badges' / name
        copy = subprocess.run(kubectl + [
            'cp', f'inspect-volumes:{badges}/', str(destination)])
        print(f'Test container exited with code {exit_code}. Job retained: {name}')
        return exit_code or copy.returncode
    except KeyboardInterrupt:
        subprocess.run(kubectl + ['delete', 'job', name, '--ignore-not-found'])
        return 130


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (subprocess.CalledProcessError, RuntimeError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
