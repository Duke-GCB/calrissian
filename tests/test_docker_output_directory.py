"""Exercise cwltool preparation and Kubernetes mapping of a fixed output path."""
import json
from pathlib import Path
from unittest.mock import patch

import pytest
from ruamel.yaml import YAML
from cwltool.errors import UnsupportedRequirement
from cwltool.process import INTERNAL_VERSION

from calrissian.context import CalrissianLoadingContext, CalrissianRuntimeContext
from calrissian.tool import CalrissianCommandLineTool
from calrissian.job import CalrissianCommandLineJob


@pytest.mark.parametrize('output_path', ['/results', '/custom/nested/results'])
def test_fixed_output_directory(tmp_path, output_path):
    spec = {
        'id': 'file:///tool.cwl', 'class': 'CommandLineTool',
        'inputs': [],
        'outputs': [{'id': 'file:///tool.cwl#report', 'type': 'File',
                     'outputBinding': {'glob': 'report.txt'}}],
        'requirements': [
            {'class': 'DockerRequirement', 'dockerPull': 'debian:stable-slim',
             'dockerOutputDirectory': output_path},
            {'class': 'InitialWorkDirRequirement', 'listing': [
                {'entryname': 'config.txt', 'entry': 'configuration'}]},
        ],
        'baseCommand': 'echo', 'arguments': ['$(runtime.outdir)'],
        'stdout': 'nested/stdout.txt',
    }
    runtime = CalrissianRuntimeContext({
        'basedir': str(tmp_path), 'outdir': str(tmp_path / 'output'),
        'tmpdir_prefix': str(tmp_path / 'tmp'),
        'tmp_outdir_prefix': str(tmp_path / 'out'),
    })
    spec = YAML().load(json.dumps(spec))
    spec['arguments'].lc.filename = 'file:///tool.cwl'
    with patch('calrissian.job.KubernetesClient'), patch('calrissian.job.KubernetesVolumeBuilder'):
        tool = CalrissianCommandLineTool(
            spec, CalrissianLoadingContext(metadata={'cwlVersion': INTERNAL_VERSION}))
        job = next(tool.job({}, lambda *args: None, runtime))
    job.check_requirements(runtime)
    assert job.builder.outdir == output_path
    assert job.outdir != output_path
    assert job.command_line == ['echo', output_path]
    job.populate_env_vars(runtime)
    assert job.environment['HOME'] == output_path
    # Build the actual Pod, while isolating persistent-volume discovery and staging.
    job.volume_builder.volume_mounts = []
    job.volume_builder.volumes = []
    with patch.object(job, '_add_volume_binding') as bind, patch.object(job, 'add_volumes'):
        pod = job.create_kubernetes_runtime(runtime)
    bind.assert_any_call(job.outdir, output_path, writable=True)
    assert pod['spec']['containers'][0]['workingDir'] == output_path
    assert pod['spec']['initContainers'][0]['workingDir'] == output_path
    assert pod['spec']['containers'][0]['securityContext']['readOnlyRootFilesystem'] is True
    assert any(entry['basename'] == 'config.txt' for entry in job.generatefiles['listing'])
    # Output collection must resolve against the runner's directory, not /results.
    Path(job.outdir).mkdir(parents=True, exist_ok=True)
    Path(job.outdir, 'report.txt').write_text('result\n')
    outputs = job.collect_outputs(job.outdir, 0)
    assert outputs['report']['location'] == Path(job.outdir, 'report.txt').as_uri()


def test_other_docker_fields_remain_unsupported():
    job = object.__new__(CalrissianCommandLineJob)
    job.requirements = [{'class': 'DockerRequirement', 'dockerPull': 'image:tag',
                         'dockerFile': 'FROM scratch'}]
    job.hints = []
    with pytest.raises(UnsupportedRequirement, match='dockerFile'):
        job.check_requirements(CalrissianRuntimeContext())
