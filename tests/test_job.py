from unittest import TestCase
from unittest.mock import Mock, patch, call
from calrissian.job import k8s_safe_name, KubernetesVolumeBuilder, VolumeBuilderException, KubernetesJobBuilder
from calrissian.job import CalrissianCommandLineJob


class SafeNameTestCase(TestCase):

    def setUp(self):
        self.unsafe_name = 'Wine_1234'
        self.safe_name = 'wine-1234'

    def test_makes_name_safe(self):
        made_safe = k8s_safe_name(self.unsafe_name)
        self.assertEqual(self.safe_name, made_safe)


class KubernetesVolumeBuilderTestCase(TestCase):

    def setUp(self):
        self.volume_builder = KubernetesVolumeBuilder()

    def test_finds_persistent_volume(self):
        self.volume_builder.add_persistent_volume_entry('/prefix/1', 'claim1')
        self.assertIsNotNone(self.volume_builder.find_persistent_volume('/prefix/1f'))
        self.assertIsNone(self.volume_builder.find_persistent_volume('/notfound'))

    def test_calculates_subpath(self):
        self.volume_builder.add_persistent_volume_entry('/prefix/1', 'claim1')
        subpath = KubernetesVolumeBuilder.calculate_subpath('/prefix/1/foo', '/prefix/1')
        self.assertEqual('foo', subpath)

    def test_random_tag(self):
        random_tag = KubernetesVolumeBuilder.random_tag(8)
        self.assertEqual(len(random_tag), 8)

    def test_add_rw_volume_binding(self):
        self.assertEqual(0, len(self.volume_builder.volumes))
        self.volume_builder.add_persistent_volume_entry('/prefix/1', 'claim1')
        self.assertEqual({'name':'claim1', 'persistentVolumeClaim': {'claimName': 'claim1'}}, self.volume_builder.volumes[0])

        self.assertEqual(0, len(self.volume_builder.volume_mounts))
        self.volume_builder.add_volume_binding('/prefix/1/input1', '/input1-target', True)
        self.assertEqual({'name': 'claim1', 'mountPath': '/input1-target', 'readOnly': False, 'subPath': 'input1'}, self.volume_builder.volume_mounts[0])

    def test_add_ro_volume_binding(self):
        # read-only
        self.assertEqual(0, len(self.volume_builder.volumes))
        self.volume_builder.add_persistent_volume_entry('/prefix/2', 'claim2')
        self.assertEqual({'name':'claim2', 'persistentVolumeClaim': {'claimName': 'claim2'}}, self.volume_builder.volumes[0])

        self.assertEqual(0, len(self.volume_builder.volume_mounts))
        self.volume_builder.add_volume_binding('/prefix/2/input2', '/input2-target', False)
        self.assertEqual({'name': 'claim2', 'mountPath': '/input2-target', 'readOnly': True, 'subPath': 'input2'}, self.volume_builder.volume_mounts[0])

    def test_volume_binding_exception_if_not_found(self):
        self.assertEqual(0, len(self.volume_builder.volumes))
        with self.assertRaises(VolumeBuilderException):
            self.volume_builder.add_volume_binding('/prefix/2/input2', '/input2-target', False)


class KubernetesJobBuilderTestCase(TestCase):

    def setUp(self):
        self.name = 'JobName'
        self.container_image = 'dockerimage:1.0'
        self.environment = {'K1':'V1', 'K2':'V2', 'HOME': '/homedir'}
        self.volume_mounts = [Mock(), Mock()]
        self.volumes = [Mock()]
        self.command_line = ['cat']
        self.stdout = 'stdout.txt'
        self.stderr = 'stderr.txt'
        self.stdin = 'stdin.txt'
        self.job_builder = KubernetesJobBuilder(self.name, self.container_image, self.environment, self.volume_mounts,
                                                self.volumes, self.command_line, self.stdout, self.stderr, self.stdin)

    def test_safe_job_name(self):
        self.assertEqual('jobname-job', self.job_builder.job_name())

    def test_safe_container_name(self):
        self.assertEqual('jobname-container', self.job_builder.container_name())

    def test_container_command(self):
        self.assertEqual(['/bin/sh', '-c'], self.job_builder.container_command())

    def test_container_args_without_redirects(self):
        # container_args returns a list with a single item since it is passed to 'sh', '-c'
        self.job_builder.stdout = None
        self.job_builder.stderr = None
        self.job_builder.stdin = None
        self.assertEqual(['cat'], self.job_builder.container_args())

    def test_container_args_with_redirects(self):
        self.assertEqual(['cat > stdout.txt 2> stderr.txt < stdin.txt'], self.job_builder.container_args())

    def test_container_environment(self):
        environment = self.job_builder.container_environment()
        self.assertEqual(len(self.environment), len(environment))
        self.assertIn({'name': 'K1', 'value': 'V1'}, environment)
        self.assertIn({'name': 'K2', 'value': 'V2'}, environment)
        self.assertIn({'name': 'HOME', 'value': '/homedir'}, environment)

    def test_container_workingdir(self):
        workingdir = self.job_builder.container_workingdir()
        self.assertEqual('/homedir', workingdir)

    def test_build(self):
        expected = {
            'metadata': {
                'name': 'jobname-job'
            },
            'apiVersion': 'batch/v1',
            'kind':'Job',
            'spec': {
                'template': {
                    'spec': {
                        'containers': [
                            {
                                'name': 'jobname-container',
                                'image': 'dockerimage:1.0',
                                'command': ['/bin/sh', '-c'],
                                'args': ['cat > stdout.txt 2> stderr.txt < stdin.txt'],
                                'env': [
                                    {'name': 'HOME', 'value': '/homedir'},
                                    {'name': 'K1', 'value': 'V1'},
                                    {'name': 'K2', 'value': 'V2'},
                                ],
                                'volumeMounts': self.volume_mounts,
                                'workingDir': '/homedir',
                             }
                        ],
                        'restartPolicy': 'Never',
                        'volumes': self.volumes
                    }
                }
            }
        }
        self.assertEqual(expected, self.job_builder.build())


@patch('calrissian.job.KubernetesClient')
@patch('calrissian.job.KubernetesVolumeBuilder')
class CalrissianCommandLineJobTestCase(TestCase):

    def setUp(self):
        self.builder = Mock(outdir='/out')
        self.joborder = Mock()
        self.make_path_mapper = Mock()
        self.requirements = [{'class': 'DockerRequirement', 'dockerPull': 'dockerimage:1.0'}]
        self.hints = []
        self.name = 'test-clj'

    def make_job(self):
        return CalrissianCommandLineJob(self.builder, self.joborder, self.make_path_mapper, self.requirements,
                                       self.hints, self.name)

    def test_init(self, mock_volume_builder, mock_client):
        job = self.make_job()
        self.assertTrue(mock_client.called)
        self.assertTrue(mock_volume_builder.called)
        self.assertTrue(mock_volume_builder.return_value.populate_demo_values.called)
        self.assertEqual(job.client, mock_client.return_value)
        self.assertEqual(job.volume_builder, mock_volume_builder.return_value)
        self.assertEqual(job.name, self.name)

    @patch('calrissian.job.os')
    def test_makes_tmpdir_when_not_exists(self, mock_os, mock_volume_builder, mock_client):
        mock_os.path.exists.return_value = False
        job = self.make_job()
        job.make_tmpdir()
        self.assertTrue(mock_os.path.exists.called)
        self.assertTrue(mock_os.makedirs.called_with(job.tmpdir))

    @patch('calrissian.job.os')
    def test_not_make_tmpdir_when_exists(self, mock_os, mock_volume_builder, mock_client):
        mock_os.path.exists.return_value = True
        job = self.make_job()
        job.make_tmpdir()
        self.assertTrue(mock_os.path.exists.called)
        self.assertFalse(mock_os.makedirs.called)

    def test_populate_env_vars(self, mock_volume_builder, mock_client):
        job = self.make_job()
        job.populate_env_vars()
        # tmpdir should be '/tmp'
        self.assertEqual(job.environment['TMPDIR'], '/tmp')
        # home should be builder.outdir
        self.assertEqual(job.environment['HOME'], '/out')

    def test_wait_for_kubernetes_job(self, mock_volume_builder, mock_client):
        job = self.make_job()
        job.wait_for_kubernetes_job()
        self.assertTrue(mock_client.return_value.wait.called)

    def test_finish_calls_output_callback_with_status(self, mock_volume_builder, mock_client):
        job = self.make_job()
        mock_collected_outputs = Mock()
        job.collect_outputs = Mock()
        job.collect_outputs.return_value = mock_collected_outputs
        job.output_callback = Mock()
        job.finish()
        self.assertTrue(job.collect_outputs.called)
        job.output_callback.assert_called_with(mock_collected_outputs, 'success')

    def test__get_container_image(self, mock_volume_builder, mock_client):
        job = self.make_job()
        image = job._get_container_image()
        self.assertEqual(image, 'dockerimage:1.0')

    @patch('calrissian.job.KubernetesJobBuilder')
    @patch('calrissian.job.os')
    def test_create_kubernetes_runtime(self, mock_os, mock_job_builder, mock_volume_builder, mock_client):
        def realpath(path):
            return '/real' + path
        mock_os.path.realpath = realpath
        mock_add_volume_binding = Mock()
        mock_volume_builder.return_value.add_volume_binding = mock_add_volume_binding

        mock_job_builder.return_value.build.return_value = '<built job>'
        job = self.make_job()
        job.outdir = '/outdir'
        job.tmpdir = '/tmpdir'
        mock_runtime_context = Mock(tmpdir_prefix='TP')
        built = job.create_kubernetes_runtime(mock_runtime_context)
        # Adds volume binding for outdir
        # Adds volume binding for /tmp
        self.assertEqual(mock_add_volume_binding.call_args_list,
                         [call('/real/outdir', '/out', True),
                          call('/real/tmpdir', '/tmp', True)])
        # looks at generatemapper
        # creates a KubernetesJobBuilder
        self.assertEqual(mock_job_builder.call_args, call(
            job.name,
            job._get_container_image(),
            job.environment,
            job.volume_builder.volume_mounts,
            job.volume_builder.volumes,
            job.command_line,
            job.stdout,
            job.stderr,
            job.stdin
        ))
        # calls builder.build
        self.assertTrue(mock_job_builder.return_value.build.called)
        # returns that
        self.assertEqual(built, mock_job_builder.return_value.build.return_value)

    def test_execute_kubernetes_job(self, mock_volume_builder, mock_client):
        job = self.make_job()
        k8s_job = Mock()
        job.execute_kubernetes_job(k8s_job)
        self.assertTrue(mock_client.return_value.submit_job.called_with(k8s_job))

    def test_add_file_or_directory_volume_ro(self, mock_volume_builder, mock_client):
        mock_add_volume_binding = mock_volume_builder.return_value.add_volume_binding
        job = self.make_job()
        runtime = []
        volume = Mock(resolved='/resolved', target='/target')
        job.add_file_or_directory_volume(runtime, volume, None)
        # It should add the volume binding with writable=False
        self.assertEqual(mock_add_volume_binding.call_args, call('/resolved', '/target', False))

    def test_ignores_add_file_or_directory_volume_with_under_colon(self, mock_volume_builder, mock_client):
        mock_add_volume_binding = mock_volume_builder.return_value.add_volume_binding
        job = self.make_job()
        runtime = []
        volume = Mock(resolved='_:/resolved', target='/target')
        job.add_file_or_directory_volume(runtime, volume, None)
        self.assertFalse(mock_add_volume_binding.called)

    def test_add_writable_file_volume_inplace(self, mock_volume_builder, mock_client):
        mock_add_volume_binding = mock_volume_builder.return_value.add_volume_binding
        job = self.make_job()
        job.inplace_update = True
        runtime = []
        volume = Mock(resolved='/resolved', target='/target')
        job.add_writable_file_volume(runtime, volume, None, None)
        # with inplace, the binding should be added with writable=True
        self.assertEqual(mock_add_volume_binding.call_args, call('/resolved', '/target', True))

    @patch('calrissian.job.shutil')
    @patch('calrissian.job.ensure_writable')
    def test_add_writable_file_volume_host_outdir_tgt(self, mock_ensure_writable, mock_shutil, mock_volume_builder, mock_client):
        mock_shutil.copy = Mock()
        mock_add_volume_binding = mock_volume_builder.return_value.add_volume_binding
        job = self.make_job()
        runtime = []
        volume = Mock(resolved='/resolved', target='/target')
        # In this case, the target is a host outdir, so we do not add a volume mapping
        # but instead just copy the file there and ensure it is writable
        job.add_writable_file_volume(runtime, volume, '/host-outdir-tgt', None)
        self.assertFalse(mock_add_volume_binding.called)
        self.assertEqual(mock_shutil.copy.call_args, call('/resolved', '/host-outdir-tgt'))
        self.assertEqual(mock_ensure_writable.call_args, call('/host-outdir-tgt'))

    @patch('calrissian.job.tempfile')
    @patch('calrissian.job.shutil')
    @patch('calrissian.job.ensure_writable')
    def test_add_writable_file_volumehost_not_outdir_tgt(self, mock_ensure_writable, mock_shutil, mock_tempfile, mock_volume_builder, mock_client):
        mock_shutil.copy = Mock()
        mock_tempfile.mkdtemp.return_value = '/mkdtemp-dir'
        mock_add_volume_binding = mock_volume_builder.return_value.add_volume_binding
        job = self.make_job()
        runtime = []
        volume = Mock(resolved='/resolved', target='/target')
        job.add_writable_file_volume(runtime, volume, None, None)
        # When writable but not inplace, we expect a copy
        # And when not host-outdir-tgt, we expect that copy in a tmpdir
        self.assertEqual(mock_tempfile.mkdtemp.call_args, call(dir=job.tmpdir))
        self.assertEqual(mock_shutil.copy.call_args, call('/resolved', '/mkdtemp-dir/resolved'))
        # and we expect add_volume_binding called with the tempdir copy
        self.assertEqual(mock_add_volume_binding.call_args, call('/mkdtemp-dir/resolved', '/target', True))
        # We also expect ensure_writable to be called on the copy in mkdtemp-dir
        self.assertEqual(mock_ensure_writable.call_args, call('/mkdtemp-dir/resolved'))

    def test_add_writable_directory_volume(self, mock_volume_builder, mock_client):
        # These were ported over from cwltool but are not used by our workflows
        # and can be tested later
        pass

    def test_run(self, mock_volume_builder, mock_client):
        job = self.make_job()
        job.make_tmpdir = Mock()
        job.populate_env_vars = Mock()
        job._setup = Mock()
        job.create_kubernetes_runtime = Mock()
        job.execute_kubernetes_job = Mock()
        job.wait_for_kubernetes_job = Mock()
        job.finish = Mock()

        runtimeContext = Mock()
        job.run(runtimeContext)
        self.assertTrue(job.make_tmpdir.called)
        self.assertTrue(job.populate_env_vars.called)
        self.assertEqual(job._setup.call_args, call(runtimeContext))
        self.assertEqual(job.create_kubernetes_runtime.call_args, call(runtimeContext))
        self.assertEqual(job.execute_kubernetes_job.call_args, call(job.create_kubernetes_runtime.return_value))
        self.assertTrue(job.wait_for_kubernetes_job.called)
        self.assertTrue(job.finish.called)
