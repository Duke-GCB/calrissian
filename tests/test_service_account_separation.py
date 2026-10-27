"""Deployment identities for runner and workflow pods must remain separate."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(shutil.which('helm') is None, reason='Helm is required to validate rendered chart identities')
@pytest.mark.parametrize('network', ['true', 'false'])
def test_chart_account_separation(network):
    documents = list(yaml.safe_load_all(subprocess.check_output([
        'helm', 'template', 'identity-test', str(ROOT / 'conformance/chart'),
        '--namespace', 'calrissian-conformance', '--set', 'networkAccess.enabled=' + network], text=True)))
    accounts = {d['metadata']['name']: d for d in documents if d and d['kind'] == 'ServiceAccount'}
    assert accounts['calrissian-sa']['automountServiceAccountToken'] is True
    assert accounts['calrissian-workflow']['automountServiceAccountToken'] is False
    bindings = [d for d in documents if d and d['kind'] in ('RoleBinding', 'ClusterRoleBinding')]
    assert len(bindings) == 1
    assert bindings[0]['subjects'] == [{'kind': 'ServiceAccount', 'name': 'calrissian-sa'}]
    config = next(d for d in documents if d and d['kind'] == 'ConfigMap' and d['metadata']['name'] == 'conformance-network-config')
    assert json.loads(config['data']['default.json'])['pod_serviceaccount'] == 'calrissian-workflow'
    session = next(d for d in documents if d and d['kind'] == 'Deployment')
    spec = session['spec']['template']['spec']
    assert spec['serviceAccountName'] == 'calrissian-sa'
    assert spec['automountServiceAccountToken'] is True


def test_all_standalone_runner_jobs_select_both_identities():
    manifests = sorted((ROOT / 'conformance').glob('ConformanceTestsJob-*.yaml'))
    assert len(manifests) == 4
    manifests.append(ROOT / 'dask-gateway/smoke-job.yaml')
    for path in manifests:
        spec = yaml.safe_load(path.read_text())['spec']['template']['spec']
        assert spec['serviceAccountName'] == 'calrissian-sa', path
        assert spec['automountServiceAccountToken'] is True, path
        args = spec['containers'][0]['args']
        assert args[args.index('--pod-serviceaccount') + 1] == 'calrissian-workflow', path
