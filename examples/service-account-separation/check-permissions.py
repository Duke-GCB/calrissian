#!/usr/bin/env python3
"""Check effective RBAC for the chart's runner, workflow, and default identities."""
import argparse
import json
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', default='calrissian-conformance')
    parser.add_argument('--namespace', default='calrissian-conformance')
    options = parser.parse_args()
    permissions = [(verb, 'pods') for verb in ('get', 'create', 'patch', 'delete', 'list', 'watch')]
    permissions += [(verb, 'pods/log') for verb in ('get', 'list')]
    permissions += [(verb, 'configmaps') for verb in ('get', 'create', 'delete')]
    results = []
    for account in ('calrissian-sa', 'calrissian-workflow', 'default'):
        expected = account == 'calrissian-sa'
        for verb, resource in permissions:
            command = ['kubectl', '--context', options.context, '--request-timeout=15s',
                       'auth', 'can-i', verb, resource, '--namespace', options.namespace,
                       '--as', f'system:serviceaccount:{options.namespace}:{account}',
                       '--as-group', 'system:serviceaccounts',
                       '--as-group', f'system:serviceaccounts:{options.namespace}',
                       '--as-group', 'system:authenticated']
            check = subprocess.run(command, capture_output=True, text=True, timeout=30)
            answer = check.stdout.strip()
            if check.returncode not in (0, 1) or answer not in ('yes', 'no'):
                raise RuntimeError(check.stderr or check.stdout)
            allowed = answer == 'yes'
            results.append({'account': account, 'verb': verb, 'resource': resource, 'allowed': allowed})
            if allowed != expected:
                raise RuntimeError(f'{account}: {verb} {resource} returned {answer}; review existing RoleBindings and ClusterRoleBindings')
    print(json.dumps({'checks': len(results), 'results': results}, indent=2))


if __name__ == '__main__':
    main()
