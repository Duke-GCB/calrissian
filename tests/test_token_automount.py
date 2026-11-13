"""Task credentials require explicit opt-in (#238)."""
from argparse import ArgumentParser
import json
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from cwltool.errors import WorkflowException
from cwltool.main import arg_parser
from calrissian.context import CalrissianRuntimeContext
from calrissian.main import add_arguments, parse_arguments
from calrissian.job import CalrissianCommandLineJob, KubernetesPodBuilder
from calrissian_dask.dask import KubernetesDaskPodBuilder


@pytest.mark.parametrize('kind', [KubernetesPodBuilder, KubernetesDaskPodBuilder])
@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('account', [None, 'workflow-account'])
def test_task_token_policy(kind, enabled, account):
    job = object.__new__(CalrissianCommandLineJob)
    runtime = CalrissianRuntimeContext({'pod_automount_service_account_token': enabled})
    builder = SimpleNamespace(cwlVersion='v1.2', resources={'cores': 1, 'ram': 512}, requirements=[], hints=[])
    parameters = ('task', builder, 'worker:immutable', {'HOME': '/work'}, [], [], ['true'],
                  None, None, None, {}, {}, {}, {}, account, job.get_pod_additional_spec(runtime))
    pod_builder = (KubernetesDaskPodBuilder('http://gateway.invalid', True, *parameters)
                   if kind is KubernetesDaskPodBuilder else KubernetesPodBuilder(*parameters))
    if kind is KubernetesDaskPodBuilder:
        pod_builder.dask_requirement = {}
    pod = pod_builder.build()
    assert pod['spec']['automountServiceAccountToken'] is enabled
    assert pod['spec'].get('serviceAccountName') == account
    assert pod['spec']['containers'][0]['resources']['requests']['cpu'] == '1'
    assert pod['spec']['securityContext'] == {}
    assert pod['spec']['volumes'] == []
    for container in pod['spec']['containers'] + pod['spec']['initContainers']:
        assert container['securityContext'] == {
            'readOnlyRootFilesystem': True, 'privileged': False, 'allowPrivilegeEscalation': False}


def test_cli_default_and_opt_in():
    parser = ArgumentParser()
    add_arguments(parser)
    for arguments, expected in [([], False), (['--pod-automount-service-account-token'], True)]:
        runtime = CalrissianRuntimeContext(vars(parser.parse_args(arguments)))
        assert runtime.pod_automount_service_account_token is expected


@pytest.mark.parametrize('value', [False, True, 'false', 'true', 0, 1, None, [], {}])
def test_json_configuration_and_validation(tmp_path, value):
    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'pod_automount_service_account_token': value}))
    parser = arg_parser()
    add_arguments(parser)
    with patch.dict('os.environ', {'HOME': str(tmp_path)}), patch.object(sys, 'argv', [
            'calrissian', '--conf', str(config), '--max-ram', '1G', '--max-cores', '1']):
        runtime = CalrissianRuntimeContext(vars(parse_arguments(parser)))
    job = object.__new__(CalrissianCommandLineJob)
    if type(value) is bool:
        assert job.get_pod_additional_spec(runtime)['automount_service_account_token'] is value
    else:
        with pytest.raises(WorkflowException, match='must be a JSON boolean'):
            job.get_pod_additional_spec(runtime)


def test_configuration_default_false():
    job = object.__new__(CalrissianCommandLineJob)
    assert job.get_pod_additional_spec(CalrissianRuntimeContext())['automount_service_account_token'] is False
