"""Conditional GPU tolerations for issue #198."""
from argparse import ArgumentParser
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import yaml
from cwltool.errors import WorkflowException

from calrissian.context import CalrissianRuntimeContext
from calrissian.dask import KubernetesDaskPodBuilder
from calrissian.job import CalrissianCommandLineJob, KubernetesPodBuilder
from calrissian.main import add_arguments
from calrissian.scheduling import load_gpu_tolerations


TOLERATIONS = [{'key': 'nvidia.com/gpu', 'operator': 'Exists', 'effect': 'NoSchedule'}]


@pytest.mark.parametrize('kind', [KubernetesPodBuilder, KubernetesDaskPodBuilder])
@pytest.mark.parametrize('configured', [False, True])
@pytest.mark.parametrize('resources,expected', [
    ({'requests': {'cpu': '1'}}, False),
    ({'limits': {'nvidia.com/gpu': '0'}}, False),
    ({'requests': {'nvidia.com/gpu': '1'}}, True),
    ({'limits': {'nvidia.com/gpu': '2'}}, True),
])
def test_conditional_pod_configuration(kind, configured, resources, expected):
    builder = SimpleNamespace(cwlVersion='v1.2', resources={'cores': 1, 'ram': 512},
                              requirements=[], hints=[{'class': 'http://commonwl.org/cwltool#CUDARequirement'}])
    args = ('task', builder, 'worker:immutable', {'HOME': '/tmp'}, [], [], ['true'], None, None, None,
            {}, {'pool': 'cpu'}, {'pool': 'gpu'}, {}, None,
            {'gpu_tolerations': TOLERATIONS if configured else None})
    pod_builder = kind('http://gateway.invalid', True, *args) if kind is KubernetesDaskPodBuilder else kind(*args)
    if kind is KubernetesDaskPodBuilder:
        pod_builder.dask_requirement = {}
    with patch.object(pod_builder, 'container_resources', return_value=resources):
        pod = pod_builder.build()
    assert pod['spec']['nodeSelector'] == {'pool': 'gpu'}
    assert pod['spec']['containers'][0]['resources'] == resources
    if expected and configured:
        assert pod['spec']['tolerations'] == TOLERATIONS
        pod['spec']['tolerations'][0]['key'] = 'modified'
        assert TOLERATIONS[0]['key'] == 'nvidia.com/gpu'
    else:
        assert 'tolerations' not in pod['spec']
    if kind is KubernetesDaskPodBuilder:
        assert 'nvidia.com/gpu' not in pod['spec']['containers'][1]['resources']['limits']


def test_cli_and_runtime_configuration(tmp_path):
    path = tmp_path / 'tolerations.yaml'
    path.write_text(yaml.safe_dump(TOLERATIONS))
    parser = ArgumentParser()
    add_arguments(parser)
    context = CalrissianRuntimeContext(vars(parser.parse_args(['--pod-gpu-tolerations', str(path)])))
    job = object.__new__(CalrissianCommandLineJob)
    assert job.get_pod_additional_spec(context)['gpu_tolerations'] == TOLERATIONS
    assert 'gpu_tolerations' not in job.get_pod_additional_spec(CalrissianRuntimeContext())


@pytest.mark.parametrize('document', [[], TOLERATIONS, [
    {'key': 'pool', 'value': 'gpu', 'effect': 'PreferNoSchedule'},
    {'operator': 'Exists'},
    {'key': 'gpu', 'operator': 'Exists', 'effect': 'NoExecute', 'tolerationSeconds': 60},
    {'key': 'gpu', 'operator': 'Exists', 'effect': 'NoExecute', 'tolerationSeconds': -1},
]])
def test_valid_configuration(tmp_path, document):
    path = tmp_path / 'tolerations.yaml'
    path.write_text(yaml.safe_dump(document))
    assert load_gpu_tolerations(path) == document


@pytest.mark.parametrize('document', [
    None, {}, ['gpu'], [{'unknown': 'field'}], [{'key': 123}],
    [{'key': 'gpu', 'operator': 'Other'}], [{'effect': 'NoSchedule'}],
    [{'key': 'gpu', 'operator': 'Exists', 'value': 'yes'}],
    [{'key': 'gpu', 'effect': 'Other'}], [{'key': 'bad/key/path'}],
    [{'key': 'BAD.EXAMPLE/gpu'}], [{'key': 'gpu', 'value': 'with spaces'}],
    [{'key': 'gpu', 'effect': 'NoExecute', 'tolerationSeconds': True}],
    [{'key': 'gpu', 'effect': 'NoExecute', 'tolerationSeconds': 2**63}],
    [{'key': 'gpu', 'tolerationSeconds': 10}],
])
def test_invalid_configuration_prevents_runtime_spec(tmp_path, document):
    path = tmp_path / 'invalid.yaml'
    path.write_text(yaml.safe_dump(document))
    context = CalrissianRuntimeContext({'pod_gpu_tolerations': str(path)})
    job = object.__new__(CalrissianCommandLineJob)
    with pytest.raises(WorkflowException, match='pod-gpu-tolerations'):
        job.get_pod_additional_spec(context)


def test_unreadable_and_malformed_files(tmp_path):
    with pytest.raises(WorkflowException, match='pod-gpu-tolerations'):
        load_gpu_tolerations(tmp_path / 'missing.yaml')
    path = tmp_path / 'malformed.yaml'
    path.write_text('[invalid: [')
    with pytest.raises(WorkflowException, match='pod-gpu-tolerations'):
        load_gpu_tolerations(path)
