"""Regression coverage for Dask/GPU schema compatibility (issue #231)."""
from pathlib import Path
import subprocess
import sys
import tempfile
import yaml
from unittest import TestCase


FIXTURES = Path(__file__).parent / 'fixtures' / 'dask_gpu'
# Run in fresh processes because CWL schema registration changes global state.
# Validation needs no cluster; only cleanup is mocked to avoid kubeconfig lookup.
VALIDATE = '''
import sys
from unittest.mock import patch
from calrissian.main import main
with patch('calrissian.main.PodMonitor.cleanup'), patch('calrissian_dask.plugin.DaskPodMonitor.cleanup'):
    sys.exit(main())
'''


class DaskGPUCompatibilityTestCase(TestCase):
    def validate(self, fixture, gateway=False, version=None, missing_cuda_version=False):
        args = [sys.executable, '-c', VALIDATE,
                '--validate', '--max-ram', '1G', '--max-cores', '1', '--max-gpus', '1']
        if gateway:
            args.extend(['--dask-gateway-url', 'http://example.invalid'])
        with tempfile.TemporaryDirectory() as directory:
            document = yaml.safe_load((FIXTURES / fixture).read_text())
            if version:
                document['cwlVersion'] = version
            if missing_cuda_version:
                del document['requirements']['cwltool:CUDARequirement']['cudaVersionMin']
            path = Path(directory) / fixture
            path.write_text(yaml.safe_dump(document))
            args.append(str(path))
            return subprocess.run(args, capture_output=True, text=True, timeout=60)

    def test_gpu_only_validates_without_gateway(self):
        result = self.validate('gpu_only.cwl')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('is valid CWL', result.stdout)

    def test_dask_only_validates_with_gateway(self):
        result = self.validate('dask_only.cwl', gateway=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('is valid CWL', result.stdout)

    def test_combined_requirements_validate_with_gateway(self):
        result = self.validate('combined.cwl', gateway=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('is valid CWL', result.stdout)

    def test_gpu_only_validates_with_gateway(self):
        result = self.validate('gpu_only.cwl', gateway=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('is valid CWL', result.stdout)

    def test_combined_requirements_validate_for_supported_versions(self):
        for version in ('v1.0', 'v1.1'):
            with self.subTest(version=version):
                result = self.validate('combined.cwl', gateway=True, version=version)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_invalid_cuda_requirement_is_still_rejected(self):
        result = self.validate('combined.cwl', gateway=True, missing_cuda_version=True)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        output = result.stdout + result.stderr
        self.assertIn('cudaVersionMin', output)
        self.assertNotIn('undefined reference', output)

    def test_schema_registration_is_repeatable(self):
        code = """
from calrissian_dask.plugin import register_dask_schema
from cwltool.process import supportedProcessRequirements
register_dask_schema()
register_dask_schema()
assert supportedProcessRequirements.count(
    'https://calrissian-cwl.github.io/schema#DaskGatewayRequirement') == 1
"""
        result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_combined_document_generates_dask_job_with_gpu_resources(self):
        code = '''
import sys
import tempfile
from unittest.mock import patch, Mock
from calrissian_dask.plugin import register_dask_schema
from calrissian.context import CalrissianLoadingContext, CalrissianRuntimeContext
from calrissian_dask.dask import CalrissianCommandLineDaskJob, KubernetesDaskPodBuilder
from calrissian.executor import ThreadPoolJobExecutor
from cwltool.load_tool import load_tool

register_dask_schema()
with tempfile.TemporaryDirectory() as directory, \\
     patch('calrissian.k8s.load_config_get_namespace', return_value='default'), \\
     patch('calrissian.k8s.KubernetesClient.get_current_pod', return_value=Mock()), \\
     patch('calrissian.job.KubernetesVolumeBuilder'):
    context = CalrissianRuntimeContext({
        'max_gpus': 1, 'basedir': directory, 'outdir': directory,
        'tmpdir_prefix': directory + '/',
        'select_resources': ThreadPoolJobExecutor(1024, 1, 1).select_resources,
    })
    context.extension_options['dask_gateway'] = {
        'url': 'http://example.invalid', 'script_configmap': None,
    }
    tool = load_tool(sys.argv[1], CalrissianLoadingContext())
    job = next(tool.job({}, Mock(), context))
    assert isinstance(job, CalrissianCommandLineDaskJob), type(job)
    job.setup_kubernetes(context)
    builder = KubernetesDaskPodBuilder(
        context.extension_options['dask_gateway']['url'], False, 'combined', job.builder, 'ubuntu:24.04',
        {'HOME': directory}, [], [], ['true'], None, None, None,
        {}, {}, {'gpu': 'true'}, {}, None, {})
    pod = builder.build()
    main = pod['spec']['containers'][0]
    assert main['resources']['requests']['nvidia.com/gpu'] == '1', main
    assert main['resources']['limits']['nvidia.com/gpu'] == '1', main
    assert pod['spec']['nodeSelector'] == {'gpu': 'true'}, pod['spec']
'''
        result = subprocess.run(
            [sys.executable, '-c', code, str(FIXTURES / 'combined.cwl')],
            capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
