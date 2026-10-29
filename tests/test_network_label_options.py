"""CLI and JSON network-label settings must reach the same runtime fields."""
import json
import sys
from unittest.mock import patch

import pytest
from cwltool.main import arg_parser
from calrissian.main import add_arguments, parse_arguments
from calrissian.context import CalrissianRuntimeContext
from calrissian.job import CalrissianCommandLineJob, KubernetesPodBuilder


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('mode', ['cli', 'json', 'override'])
def test_network_label_configuration_reaches_task_pod(tmp_path, enabled, mode):
    allowed = tmp_path / 'allowed.yaml'
    denied = tmp_path / 'denied.yaml'
    allowed.write_text('test.example/network: allowed\n')
    denied.write_text('test.example/network: denied\n')
    config = tmp_path / 'config.json'
    values = {'network_access_pod_labels': str(allowed), 'no_network_access_pod_labels': str(denied)}
    # Missing paths make a failure to override configuration defaults observable.
    config.write_text(json.dumps(values if mode == 'json' else {k: '/missing/' + k for k in values}))
    arguments = ['calrissian', '--max-ram', '1G', '--max-cores', '1']
    if mode != 'cli':
        arguments += ['--conf', str(config)]
    if mode != 'json':
        arguments += ['--network-access-pod-label', str(allowed), '--no-network-access-pod-label', str(denied)]
    parser = arg_parser()
    add_arguments(parser)
    with patch.dict('os.environ', {'HOME': str(tmp_path)}), patch.object(sys, 'argv', arguments):
        parsed = parse_arguments(parser)
    runtime = CalrissianRuntimeContext(vars(parsed))
    assert runtime.network_access_pod_labels == str(allowed)
    assert runtime.no_network_access_pod_labels == str(denied)
    job = object.__new__(CalrissianCommandLineJob)
    # Exercise the actual YAML loading and label selection without Kubernetes access.
    builder = object.__new__(KubernetesPodBuilder)
    builder.cwl_version = 'v1.2'
    builder.requirements = [{'class': 'NetworkAccess', 'networkAccess': enabled}]
    builder.labels = {}
    builder.network_access_pod_labels = job.get_network_access_pod_labels(runtime)
    builder.no_network_access_pod_labels = job.get_no_network_access_pod_labels(runtime)
    assert builder.pod_labels() == {'test.example/network': 'allowed' if enabled else 'denied'}


def test_network_label_options_default_to_unconfigured():
    parser = arg_parser()
    add_arguments(parser)
    runtime = CalrissianRuntimeContext(vars(parser.parse_args([])))
    assert runtime.network_access_pod_labels is None
    assert runtime.no_network_access_pod_labels is None
