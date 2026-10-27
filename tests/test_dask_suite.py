"""Regression checks for the host-side extension suite controller."""
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('dask_suite', ROOT / 'dask-gateway/run-tests.py')
suite = importlib.util.module_from_spec(spec)
spec.loader.exec_module(suite)


def test_cluster_shutdown_checks_exact_identity():
    clusters = {'items': [
        {'metadata': {'name': 'owned'}, 'status': {'phase': 'Stopped'}},
        {'metadata': {'name': 'unrelated'}, 'status': {'phase': 'Running'}},
    ]}
    def read(*args):
        return clusters if 'daskclusters' in args else {'items': []}
    with patch.object(suite, 'read_json', side_effect=read):
        assert suite.stopped('owned')
        assert suite.stopped('deleted')
        assert not suite.stopped('unrelated')


def test_stopped_cluster_with_remaining_pods_is_not_clean():
    with patch.object(suite, 'read_json', side_effect=[
        {'items': [{'metadata': {'name': 'owned'}, 'status': {'phase': 'Stopped'}}]},
        {'items': [{'metadata': {'name': 'remaining-worker'}}]},
    ]):
        assert not suite.stopped('owned')


def test_suite_ids_and_fixtures():
    root = ROOT / 'dask-gateway/tests'
    cases = json.loads((root / 'suite.json').read_text())
    assert len({c['id'] for c in cases}) == len(cases)
    for case in cases:
        tool = json.loads((root / case['tool']).read_text())
        assert tool['cwlVersion'] == 'v1.2'
    assert next(c for c in cases if c['name'] == 'ordinary')['clusters'] == 0
    assert next(c for c in cases if c['name'] == 'command-failure')['should_fail']
    assert next(c for c in cases if c['name'] == 'sequential-workflow')['clusters'] == 2




def test_cwl_failure_does_not_hide_successful_lifecycle_cleanup(tmp_path):
    import copy
    manifest = {'metadata': {}, 'spec': {'template': {'spec': {
        'containers': [{'args': ['--default-container', 'old', '--dask-script-configmap', 'old',
                                 '--tool-logs-basepath', 'old', '--outdir', 'old', 'tool.cwl']}],
        'volumes': []}}}}
    def read(*args):
        if 'configmap' in args:
            return {'data': {}}
        if 'create' in args:
            return copy.deepcopy(manifest)
        if 'pods' in args:
            return {'items': []}
        if 'job' in args:
            return {'status': {'failed': 1}}
        raise AssertionError(args)
    case = {'id': 5, 'name': 'ordinary', 'tool': 'ordinary.cwl', 'clusters': 0, 'should_fail': False}
    with patch.object(suite, 'read_json', side_effect=read), patch.object(suite, 'command', return_value='') as command:
        result = suite.run_case(case, 'worker:immutable', tmp_path, 30)
    assert result['cwl'] == 'failed'
    assert result['lifecycle'] == 'passed'
    assert 'Unexpected CWL exit status' in result['error']
    deletes = [c.args for c in command.call_args_list if 'delete' in c.args]
    assert any('job' in c and result['job'] in c for c in deletes)
    assert any('configmap' in c and result['job'] in c for c in deletes)
    assert all('--all' not in c for c in deletes)


def test_interrupt_recovers_cluster_identity_from_inflight_init(tmp_path):
    manifest = {'metadata': {}, 'spec': {'template': {'spec': {
        'containers': [{'args': ['--default-container', 'old', '--dask-script-configmap', 'old',
                                 '--tool-logs-basepath', 'old', '--outdir', 'old', 'tool.cwl']}],
        'volumes': []}}}}
    polls = 0
    def read(*args):
        nonlocal polls
        if 'configmap' in args:
            return {'data': {}}
        if 'create' in args:
            return manifest
        if 'pods' in args:
            polls += 1
            if polls == 1:
                raise KeyboardInterrupt()
            return {'items': [{'metadata': {'name': 'owned-task'}, 'status': {
                'initContainerStatuses': [{'name': 'init', 'state': {'running': {}}}]}}]}
        raise AssertionError(args)
    def command(*args, **kwargs):
        if '--follow' in args:
            return 'Cluster name dask-gateway.owned-cluster saved to /shared/name'
        return ''
    case = {'id': 1, 'tool': 'minimal.cwl', 'clusters': 1, 'should_fail': False}
    with patch.object(suite, 'read_json', side_effect=read), \
            patch.object(suite, 'command', side_effect=command) as calls, \
            patch.object(suite, 'stopped', return_value=False):
        result = suite.run_case(case, 'worker:immutable', tmp_path, 30)
    assert result['interrupted']
    assert result['clusters_observed'] == ['owned-cluster']
    shutdowns = [c for c in calls.call_args_list if 'exec' in c.args]
    assert len(shutdowns) == 1
    assert 'dask-gateway.owned-cluster' in shutdowns[0].args[-1]
    assert 'cluster.shutdown()' in shutdowns[0].args[-1]
