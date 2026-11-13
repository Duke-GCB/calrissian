"""Security contexts must use Kubernetes' pod and container scopes (#237)."""
from types import SimpleNamespace
import os

import pytest
from calrissian.job import CalrissianCommandLineJob, KubernetesPodBuilder
from calrissian_dask.dask import KubernetesDaskPodBuilder
from calrissian.context import CalrissianRuntimeContext


@pytest.mark.parametrize('kind', [KubernetesPodBuilder, KubernetesDaskPodBuilder])
@pytest.mark.parametrize('nested', [False, True])
@pytest.mark.parametrize('no_read_only', [False, True])
@pytest.mark.parametrize('no_match_user', [False, True])
def test_security_context_scope_and_overrides(kind, nested, no_read_only, no_match_user):
    runtime = CalrissianRuntimeContext({'no_read_only': no_read_only, 'no_match_user': no_match_user})
    job = object.__new__(CalrissianCommandLineJob)
    builder = SimpleNamespace(cwlVersion='v1.2', resources={'cores': 1, 'ram': 512}, requirements=[], hints=[])
    arguments = ('security', builder, 'worker:immutable', {'HOME': '/work'}, [], [], ['true'],
                 'nested/result.txt' if nested else None, None, None, {}, {}, {},
                 job.get_security_context(runtime), None, job.get_pod_additional_spec(runtime))
    pod_builder = (KubernetesDaskPodBuilder('http://gateway.invalid', True, *arguments)
                   if kind is KubernetesDaskPodBuilder else kind(*arguments))
    if kind is KubernetesDaskPodBuilder:
        pod_builder.dask_requirement = {}
    pod = pod_builder.build()
    assert pod['spec']['securityContext'] == ({} if no_match_user else {'runAsUser': os.getuid(), 'runAsGroup': os.getgid()})
    containers = pod['spec']['containers'] + pod['spec']['initContainers']
    assert len({c['name'] for c in pod['spec']['initContainers']}) == len(pod['spec']['initContainers'])
    expected = {'readOnlyRootFilesystem': not no_read_only, 'privileged': False, 'allowPrivilegeEscalation': False}
    for container in containers:
        assert container['securityContext'] == expected
    containers[0]['securityContext']['privileged'] = True
    assert pod_builder.container_security_context == expected
    assert all(c['securityContext'] == expected for c in containers[1:])
