import os
from unittest import TestCase
from unittest.mock import Mock, patch, call, create_autospec
from calrissian.job import k8s_safe_name, KubernetesVolumeBuilder, VolumeBuilderException, KubernetesPodBuilder, random_tag, read_yaml
from calrissian.job import CalrissianCommandLineJob, KubernetesPodVolumeInspector, CalrissianCommandLineJobException, total_size, quoted_arg_list
from calrissian.job import INIT_IMAGE_ENV_VARIABLE, DEFAULT_INIT_IMAGE
from cwltool.errors import UnsupportedRequirement
from calrissian.context import CalrissianRuntimeContext
from calrissian.k8s import CompletionResult
import threading


class SafeNameTestCase(TestCase):

    def setUp(self):
        self.unsafe_name = 'Wine_1234.foo'
        self.safe_name = 'wine-1234-foo'

    def test_makes_name_safe(self):
        made_safe = k8s_safe_name(self.unsafe_name)
        self.assertEqual(self.safe_name, made_safe)


class RandomTagTestCase(TestCase):

    def test_makes_random_tags(self):
        tag1, tag2 = random_tag(10), random_tag(10)
        self.assertEqual(len(tag1), 10)
        self.assertEqual(len(tag2), 10)
        self.assertNotEqual(tag1, tag2)


class ReadYamlTestCase(TestCase):

    @patch('builtins.open')
    @patch('calrissian.job.yaml')
    def test_read_yaml(self, mock_yaml, mock_open):
        mock_result = Mock()
        mock_yaml.safe_load.return_value = mock_result
        result = read_yaml('filename.yaml')
        self.assertEqual(result, mock_result)
        self.assertEqual(mock_open.call_args, call('filename.yaml'))
        self.assertEqual(mock_yaml.safe_load.call_args, call(mock_open.return_value.__enter__.return_value))


class QuotedArgListTestCase(TestCase):

    def test_quoted_arg_list(self):
        arg_list = ['ls', '@foo']
        self.assertEqual(quoted_arg_list(arg_list), ['ls', '\'@foo\''])


class KubernetesPodVolumeInspectorTestCase(TestCase):
    def test_first_container_with_one_container(self):
        mock_pod = Mock()
        mock_pod.spec.containers = ['somecontainer']
        kpod = KubernetesPodVolumeInspector(mock_pod)
        container = kpod.get_first_container()
        self.assertEqual(container, 'somecontainer')

    def test_first_container_with_two_container(self):
        mock_pod = Mock()
        mock_pod.spec.containers = ['container1', 'container2']
        kpod = KubernetesPodVolumeInspector(mock_pod)
        container = kpod.get_first_container()
        self.assertEqual(container, 'container1')

    def test_get_persistent_volumes_dict(self):
        mock_pod = Mock()
        kpod = KubernetesPodVolumeInspector(mock_pod)
        mock_data1_volume = Mock()
        mock_data1_volume.name = 'data1'
        mock_data1_volume.persistent_volume_claim = Mock(claim_name='data1-claim-name', read_only=False)
        mock_data2_volume = Mock()
        mock_data2_volume.name = 'data2'
        mock_data2_volume.persistent_volume_claim = Mock(claim_name='data2-claim-name', read_only=False)

        mock_ignored_volume = Mock(persistent_volume_claim=Mock())
        mock_pod.spec.volumes = [mock_data1_volume, mock_data2_volume]
        expected_dict = {
            'data1': ('data1-claim-name', False),
            'data2': ('data2-claim-name', False),
        }
        self.assertEqual(kpod.get_persistent_volumes_dict(), expected_dict)

    def test_get_mounted_persistent_volumes(self):
        mock_pod = Mock()
        mock_volume_mount1 = Mock()
        mock_volume_mount1.name = 'data1'
        mock_volume_mount1.mount_path = '/data/one'
        mock_volume_mount1.sub_path = None
        mock_volume_mount2 = Mock()
        mock_volume_mount2.name = 'data2'
        mock_volume_mount2.mount_path = '/data/two'
        mock_volume_mount2.sub_path = '/basedir'
        mock_pod.spec.containers = [
            Mock(volume_mounts=[mock_volume_mount1, mock_volume_mount2])
        ]
        kpod = KubernetesPodVolumeInspector(mock_pod)
        kpod.get_persistent_volumes_dict = Mock()
        kpod.get_persistent_volumes_dict.return_value = {'data1': ('data1-claim', False), 'data2': ('data2-claim', False)}
        mp_volumes = kpod.get_mounted_persistent_volumes()

        self.assertEqual(mp_volumes, [
            ('/data/one', None, 'data1-claim', False),
            ('/data/two', '/basedir', 'data2-claim', False)
        ])

    def test_get_mounted_persistent_volumes_ignores_unmounted_volumes(self):
        mock_pod = Mock()
        mock_volume_mount1 = Mock()
        mock_volume_mount1.name = 'data1'
        mock_volume_mount1.mount_path = '/data/one'
        mock_volume_mount1.sub_path = None
        mock_volume_mount2 = Mock()
        mock_volume_mount2.name = 'data2'
        mock_volume_mount2.mount_path = '/data/two'
        mock_volume_mount2.sub_path = 'basedir'
        mock_pod.spec.containers = [
            Mock(volume_mounts=[mock_volume_mount1, mock_volume_mount2])
        ]
        kpod = KubernetesPodVolumeInspector(mock_pod)
        kpod.get_persistent_volumes_dict = Mock()
        kpod.get_persistent_volumes_dict.return_value = {'data1': ('data1-claim', False)}
        mp_volumes = kpod.get_mounted_persistent_volumes()

        self.assertEqual(mp_volumes, [('/data/one', None, 'data1-claim', False)])


class KubernetesVolumeBuilderTestCase(TestCase):

    def setUp(self):
        self.volume_builder = KubernetesVolumeBuilder()

    def test_finds_persistent_volume(self):
        self.volume_builder.add_persistent_volume_entry('/prefix/1', None, 'claim1', False)
        self.assertIsNotNone(self.volume_builder.find_persistent_volume('/prefix/1f'))
        self.assertIsNone(self.volume_builder.find_persistent_volume('/notfound'))

    def test_calculates_subpath(self):
        subpath = KubernetesVolumeBuilder.calculate_subpath('/prefix/1/foo', '/prefix/1', None)
        self.assertEqual('foo', subpath)

    def test_calculates_subpath_with_parent_subpath(self):
        subpath = KubernetesVolumeBuilder.calculate_subpath('/prefix/1/foo', '/prefix/1', 'basedir')
        self.assertEqual('basedir/foo', subpath)

    def test_calculates_subpath_with_parent_subpath_with_trailing_slash(self):
        subpath = KubernetesVolumeBuilder.calculate_subpath('/prefix/1/foo', '/prefix/1/', 'basedir')
        self.assertEqual('basedir/foo', subpath)

    def test_add_rw_volume_binding(self):
        self.assertEqual(0, len(self.volume_builder.volumes))
        self.volume_builder.add_persistent_volume_entry('/prefix/1', None, 'claim1', False)
        self.assertEqual({'name':'claim1', 'persistentVolumeClaim': {'claimName': 'claim1', 'readOnly': False}}, self.volume_builder.volumes[0])

        self.assertEqual(0, len(self.volume_builder.volume_mounts))
        self.volume_builder.add_volume_binding('/prefix/1/input1', '/input1-target', True)
        self.assertEqual({'name': 'claim1', 'mountPath': '/input1-target', 'readOnly': False, 'subPath': 'input1'}, self.volume_builder.volume_mounts[0])

    def test_add_ro_volume_binding(self):
        # read-only
        self.assertEqual(0, len(self.volume_builder.volumes))
        self.volume_builder.add_persistent_volume_entry('/prefix/2', None, 'claim2', True)
        self.assertEqual({'name':'claim2', 'persistentVolumeClaim': {'claimName': 'claim2', 'readOnly': True}}, self.volume_builder.volumes[0])

        self.assertEqual(0, len(self.volume_builder.volume_mounts))
        self.volume_builder.add_volume_binding('/prefix/2/input2', '/input2-target', False)
        self.assertEqual({'name': 'claim2', 'mountPath': '/input2-target', 'readOnly': True, 'subPath': 'input2'}, self.volume_builder.volume_mounts[0])

    def test_volume_binding_exception_if_not_found(self):
        self.assertEqual(0, len(self.volume_builder.volumes))
        with self.assertRaisesRegex(VolumeBuilderException, 'Could not find a persistent volume'):
            self.volume_builder.add_volume_binding('/prefix/2/input2', '/input2-target', False)

    def test_add_emptydir_volume(self):
        self.assertEqual(0, len(self.volume_builder.emptydir_volume_names))
        self.volume_builder.add_emptydir_volume('empty-volume')
        self.assertIn('empty-volume', self.volume_builder.emptydir_volume_names)

    def test_add_emptydir_volume_binding(self):
        self.volume_builder.add_emptydir_volume('empty-volume')
        self.volume_builder.add_emptydir_volume_binding('empty-volume', '/path/to/empty')
        expected = {'name': 'empty-volume', 'mountPath': '/path/to/empty'}
        self.assertIn(expected, self.volume_builder.volume_mounts)

    def test_add_emptydir_volume_binding_exception_if_not_found(self):
        self.assertEqual(0, len(self.volume_builder.emptydir_volume_names))
        with self.assertRaises(VolumeBuilderException):
            self.volume_builder.add_emptydir_volume_binding('empty-volume', '/path/to/empty')

    @patch('calrissian.job.KubernetesPodVolumeInspector')
    def test_add_persistent_volume_entries_from_pod(self, mock_kubernetes_pod_inspector):
        mock_kubernetes_pod_inspector.return_value.get_mounted_persistent_volumes.return_value = [
            ('/tmp/data1', None, 'data1-claim', False),
            ('/tmp/data2', '/basedir', 'data2-claim', False),
        ]

        self.volume_builder.add_persistent_volume_entries_from_pod('some-pod-data')

        pv_entries = self.volume_builder.persistent_volume_entries
        self.assertEqual(pv_entries.keys(), set(['/tmp/data1', '/tmp/data2']))
        expected_entry1 = {
            'prefix': '/tmp/data1',
            'subPath': None,
            'volume': {
                'name': 'data1-claim',
                'persistentVolumeClaim': {
                    'claimName': 'data1-claim',
                    'readOnly': False
                }
            }
        }
        expected_entry2 = {
            'prefix': '/tmp/data2',
            'subPath': '/basedir',
            'volume': {
                'name': 'data2-claim',
                'persistentVolumeClaim': {
                    'claimName': 'data2-claim',
                    'readOnly': False
                }
            }
        }
        self.assertEqual(pv_entries['/tmp/data1'], expected_entry1)
        self.assertEqual(pv_entries['/tmp/data2'], expected_entry2)
        volumes = self.volume_builder.volumes
        self.assertEqual(len(volumes), 2)
        self.assertEqual(volumes[0], expected_entry1['volume'])
        self.assertEqual(volumes[1], expected_entry2['volume'])

    @patch('calrissian.job.KubernetesPodVolumeInspector')
    def test_add_persistent_volume_entries_from_pod_preserves_readonly(self, mock_kubernetes_pod_inspector):
        mock_kubernetes_pod_inspector.return_value.get_mounted_persistent_volumes.return_value = [
            ('/tmp/readonly', None, 'readonly-claim', True),
            ('/tmp/writable', '/basedir', 'writable-claim', False),
        ]

        self.volume_builder.add_persistent_volume_entries_from_pod('some-pod-data')

        pv_entries = self.volume_builder.persistent_volume_entries
        self.assertEqual(pv_entries.keys(), set(['/tmp/readonly', '/tmp/writable']))
        expected_entry1 = {
            'prefix': '/tmp/readonly',
            'subPath': None,
            'volume': {
                'name': 'readonly-claim',
                'persistentVolumeClaim': {
                    'claimName': 'readonly-claim',
                    'readOnly': True,
                },
            }
        }
        expected_entry2 = {
            'prefix': '/tmp/writable',
            'subPath': '/basedir',
            'volume': {
                'name': 'writable-claim',
                'persistentVolumeClaim': {
                    'claimName': 'writable-claim',
                    'readOnly': False,
                }
            }
        }
        self.assertEqual(pv_entries['/tmp/readonly'], expected_entry1)
        self.assertEqual(pv_entries['/tmp/writable'], expected_entry2)
        volumes = self.volume_builder.volumes
        self.assertEqual(len(volumes), 2)
        self.assertEqual(volumes[0], expected_entry1['volume'])
        self.assertEqual(volumes[1], expected_entry2['volume'])


class KubernetesPodBuilderTestCase(TestCase):

    def setUp(self):
        self.name = 'PodName'
        self.container_image = 'dockerimage:1.0'
        self.environment = {'K1':'V1', 'K2':'V2', 'HOME': '/homedir'}
        self.volume_mounts = [Mock(), Mock()]
        self.volumes = [Mock()]
        self.command_line = ['cat']
        self.stdout = 'stdout.txt'
        self.stderr = 'stderr.txt'
        self.stdin = 'stdin.txt'
        self.resources = {'cores': 1, 'ram': 1024}
        self.labels = {'key1': 'val1', 'key2': 123}
        self.nodeselectors = {'disktype': 'ssd', 'cachelevel': 2}
        self.security_context = { 'runAsUser': os.getuid(),'runAsGroup': os.getgid() }
        self.pod_serviceaccount = "podmanager"
        self.pod_builder = KubernetesPodBuilder(self.name, self.container_image, self.environment, self.volume_mounts,
                                                self.volumes, self.command_line, self.stdout, self.stderr, self.stdin,
                                                self.resources, self.labels, self.nodeselectors, self.security_context, self.pod_serviceaccount)

    @patch('calrissian.job.random_tag')
    def test_safe_pod_name(self, mock_random_tag):
        mock_random_tag.return_value = 'random'
        self.assertEqual('podname-pod-random', self.pod_builder.pod_name())

    def test_safe_container_name(self):
        self.assertEqual('podname-container', self.pod_builder.container_name())

    def test_container_command(self):
        self.assertEqual(['/bin/sh', '-c'], self.pod_builder.container_command())

    def test_container_args_without_redirects(self):
        # container_args returns a list with a single item since it is passed to 'sh', '-c'
        self.pod_builder.stdout = None
        self.pod_builder.stderr = None
        self.pod_builder.stdin = None
        self.assertEqual(['cat'], self.pod_builder.container_args())

    def test_container_args_with_redirects(self):
        self.assertEqual(['cat > stdout.txt 2> stderr.txt < stdin.txt'], self.pod_builder.container_args())

    def test_container_environment(self):
        environment = self.pod_builder.container_environment()
        self.assertEqual(len(self.environment), len(environment))
        self.assertIn({'name': 'K1', 'value': 'V1'}, environment)
        self.assertIn({'name': 'K2', 'value': 'V2'}, environment)
        self.assertIn({'name': 'HOME', 'value': '/homedir'}, environment)

    def test_container_workingdir(self):
        workingdir = self.pod_builder.container_workingdir()
        self.assertEqual('/homedir', workingdir)

    def test_resource_type(self):
        self.assertEqual('cpu', KubernetesPodBuilder.resource_type('cores'))
        self.assertEqual('memory', KubernetesPodBuilder.resource_type('ram'))
        self.assertEqual(None, KubernetesPodBuilder.resource_type('outdir'))

    def test_resource_value(self):
        self.assertEqual('3Mi', KubernetesPodBuilder.resource_value('memory', 3))
        self.assertEqual('4', KubernetesPodBuilder.resource_value('cpu', 4))
        self.assertEqual(None, KubernetesPodBuilder.resource_value('outdir', 62))

    def test_container_resources(self):
        self.pod_builder.resources = {'cores': 2, 'ram': 256}
        resources = self.pod_builder.container_resources()
        expected = {
            'requests': {
                'cpu': '2',
                'memory': '256Mi'
            }
        }
        self.assertEqual(expected, resources)

    def test_string_labels(self):
        self.pod_builder.labels = {'key1': 123}
        self.assertEqual(self.pod_builder.pod_labels(), {'key1':'123'})
        
    def test_string_nodeselectors(self):
        self.pod_builder.nodeselectors = {'cachelevel': 2}
        self.assertEqual(self.pod_builder.pod_nodeselectors(), {'cachelevel':'2'})

    def test_init_containers_empty_when_no_stdout_or_stderr(self):
        self.pod_builder.stdout = None
        self.pod_builder.stderr = None
        self.assertEqual(len(self.pod_builder.init_containers()), 0)

    def test_init_containers_empty_when_stdout_is_local_file(self):
        self.pod_builder.stdout = 'stdout.txt'
        self.pod_builder.stderr = None
        self.assertEqual(len(self.pod_builder.init_containers()), 0)

    def test_init_containers_empty_when_stderr_is_local_file(self):
        self.pod_builder.stdout = None
        self.pod_builder.stderr = 'stderr.txt'
        self.assertEqual(len(self.pod_builder.init_containers()), 0)

    def test_init_containers_when_stdout_has_path(self):
        self.pod_builder.stdout = 'out/to/stdout.txt'
        self.pod_builder.stderr = 'err/to/stderr.txt'
        init_containers = self.pod_builder.init_containers()
        self.assertEqual(len(init_containers), 1)
        container = init_containers[0]
        self.assertEqual(container['name'], 'podname-init')
        self.assertEqual(container['image'], DEFAULT_INIT_IMAGE)
        self.assertEqual(container['command'], ['/bin/sh','-c','mkdir -p out/to; mkdir -p err/to;'])
        self.assertEqual(container['volumeMounts'], self.pod_builder.volume_mounts)

    @patch('calrissian.job.os.environ.get')
    def test_init_container_name_default(self, mock_environ_get):
        mock_environ_get.return_value = 'custom-init:1.0'
        self.pod_builder.stdout = 'out/to/stdout.txt'
        self.pod_builder.stderr = 'err/to/stderr.txt'
        init_containers = self.pod_builder.init_containers()
        container = init_containers[0]
        self.assertEqual(container['image'], 'custom-init:1.0')
        self.assertEqual(mock_environ_get.call_args, call(INIT_IMAGE_ENV_VARIABLE, DEFAULT_INIT_IMAGE))

    @patch('calrissian.job.random_tag')
    def test_build(self, mock_random_tag):
        mock_random_tag.return_value = 'random'
        expected = {
            'metadata': {
                'name': 'podname-pod-random',
                'labels': {
                    'key1': 'val1',
                    'key2': '123',
                }
            },
            'apiVersion': 'v1',
            'kind':'Pod',
            'spec': {
                'initContainers': [],
                'containers': [
                    {
                        'name': 'podname-container',
                        'image': 'dockerimage:1.0',
                        'command': ['/bin/sh', '-c'],
                        'args': ['cat > stdout.txt 2> stderr.txt < stdin.txt'],
                        'env': [
                            {'name': 'HOME', 'value': '/homedir'},
                            {'name': 'K1', 'value': 'V1'},
                            {'name': 'K2', 'value': 'V2'},
                        ],
                        'resources': {
                            'requests': {
                                'cpu': '1',
                                'memory': '1024Mi',
                            }
                        },
                        'volumeMounts': self.volume_mounts,
                        'workingDir': '/homedir',
                     }
                ],
                'restartPolicy': 'Never',
                'volumes': self.volumes,
                'nodeSelector': {
                    "disktype": "ssd",
                    "cachelevel": "2"
                },
                'securityContext': {
                    'runAsUser': os.getuid(),
                    'runAsGroup': os.getgid()
                },
                'serviceAccountName': 'podmanager'
            }
        }
        self.assertEqual(expected, self.pod_builder.build())


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
        self.runtime_context = CalrissianRuntimeContext({'workflow_eval_lock': threading.Lock()})

    def make_job(self):
        job = CalrissianCommandLineJob(self.builder, self.joborder, self.make_path_mapper, self.requirements,
                                       self.hints, self.name)
        mock_collected_outputs = Mock()
        job.collect_outputs = Mock()
        job.collect_outputs.return_value = mock_collected_outputs
        job.output_callback = Mock()
        return job

    def make_completion_result(self, exit_code):
        return create_autospec(CompletionResult, pod_name=self.name, exit_code=exit_code, cpus='1', memory='1', start_time=Mock(),
                        finish_time=Mock(), pod_log='logs/')

    def test_constructor_calculates_persistent_volume_entries(self, mock_volume_builder, mock_client):
        self.make_job()
        mock_volume_builder.return_value.add_persistent_volume_entries_from_pod.assert_called_with(
            mock_client.return_value.get_current_pod.return_value
        )

    def test_check_requirements_raises_with_docker_build(self, mock_volume_builder, mock_client):
        self.requirements = [{'class': 'DockerRequirement', 'dockerBuild': 'FROM ubuntu:latest\n'}]
        job = self.make_job()
        with self.assertRaisesRegex(UnsupportedRequirement, 'DockerRequirement.dockerBuild is not supported'):
            job.check_requirements()

    def test_check_requirements_ok_with_empty_requirements(self, mock_volume_builder, mock_client):
        self.requirements = []
        job = self.make_job()
        job.check_requirements()

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

    @patch('calrissian.job.read_yaml')
    def test_populate_env_vars(self, mock_read_yaml, mock_volume_builder, mock_client):
        expected_env_vars = {'HTTP_PROXY':'http://1.2.3.4:3853'}
        mock_read_yaml.return_value = expected_env_vars
        mock_runtime_context = Mock(pod_env_vars='env_vars.yaml')
        job = self.make_job()
        job.populate_env_vars(mock_runtime_context)
        # tmpdir should be '/tmp'
        self.assertEqual(job.environment['TMPDIR'], '/tmp')
        # home should be builder.outdir
        self.assertEqual(job.environment['HOME'], '/out')
        # HTTP_PROXY should be set
        self.assertEqual(job.environment['HTTP_PROXY'], 'http://1.2.3.4:3853')

    def test_wait_for_kubernetes_pod(self, mock_volume_builder, mock_client):
        job = self.make_job()
        job.wait_for_kubernetes_pod()
        self.assertTrue(mock_client.return_value.wait_for_completion.called)

    @patch('calrissian.job.Reporter')
    def test_finish_calls_output_callback_with_status(self, mock_reporter, mock_volume_builder, mock_client):
        job = self.make_job()
        completion_result = self.make_completion_result(0) # 0 = exit success
        job.finish(completion_result, self.runtime_context)
        self.assertEqual(job.collect_outputs.call_args, call(job.outdir, 0))
        job.output_callback.assert_called_with(job.collect_outputs.return_value, 'success')

    @patch('calrissian.job.Reporter')
    def test_finish_looks_up_codes(self, mock_reporter, mock_volume_builder, mock_client):
        job = self.make_job()
        job.successCodes = [1,] # Also 0
        job.temporaryFailCodes = [2,]
        job.permanentFailCodes = [3,] # also anything not covered
        expected_codes = {
            0: 'success',
            1: 'success',
            2: 'temporaryFail',
            3: 'permanentFail',
            4: 'permanentFail',
            -1: 'permanentFail'
        }
        for code, status in expected_codes.items():
            completion_result = self.make_completion_result(code)
            job.finish(completion_result, self.runtime_context)
            job.output_callback.assert_called_with(job.collect_outputs.return_value, status)

    @patch('calrissian.job.Reporter')
    @patch('calrissian.job.os')
    @patch('calrissian.job.shutil')
    def test_finish_removes_stagedir(self, mock_shutil, mock_os, mock_reporter, mock_volume_builder, mock_client):
        mock_os.path.exists.return_value = True
        job = self.make_job()
        job.stagedir = 'stagedir'
        completion_result = self.make_completion_result(0)
        job.finish(completion_result, self.runtime_context)
        self.assertIn(call('stagedir', True), mock_shutil.rmtree.mock_calls)
        self.assertEqual(mock_os.path.exists.call_args, call('stagedir'))

    @patch('calrissian.job.Reporter')
    @patch('calrissian.job.shutil')
    def test_finish_removes_tmpdir(self, mock_shutil, mock_reporter, mock_volume_builder, mock_client):
        job = self.make_job()
        job.tmpdir = 'tmpdir'
        self.runtime_context.rm_tmpdir = True
        completion_result = self.make_completion_result(0)
        job.finish(completion_result, self.runtime_context)
        self.assertIn(call('tmpdir', True), mock_shutil.rmtree.mock_calls)

    @patch('calrissian.job.Reporter')
    @patch('calrissian.job.shutil')
    def test_finish_leaves_tmpdir(self,  mock_shutil, mock_reporter, mock_volume_builder, mock_client):
        job = self.make_job()
        job.tmpdir = 'tmpdir'
        self.runtime_context.rm_tmpdir = False
        completion_result = self.make_completion_result(0)
        job.finish(completion_result, self.runtime_context)
        self.assertNotIn(call('tmpdir', True), mock_shutil.rmtree.mock_calls)

    def test__get_container_image_docker_pull(self, mock_volume_builder, mock_client):
        job = self.make_job()
        image = job._get_container_image()
        self.assertEqual(image, 'dockerimage:1.0')

    def test__get_container_image_find_default(self, mock_volume_builder, mock_client):
        self.requirements = [] # Clear out the dockerimage:1.0 from our requirements
        self.builder.find_default_container.return_value = 'default:tag'
        job = self.make_job()
        image = job._get_container_image()
        self.assertEqual(image, 'default:tag')

    def test__get_container_image_raises_no_default(self, mock_volume_builder, mock_client):
        self.requirements = [] # Clear out the dockerimage:1.0 from our requirements
        self.builder.find_default_container.return_value = None
        job = self.make_job()
        with self.assertRaisesRegex(CalrissianCommandLineJobException, 'Please ensure tool has a DockerRequirement with dockerPull'):
            job._get_container_image()

    @patch('calrissian.job.KubernetesPodBuilder')
    @patch('calrissian.job.os')
    @patch('calrissian.job.read_yaml')
    def test_create_kubernetes_runtime(self, mock_read_yaml, mock_os, mock_pod_builder, mock_volume_builder, mock_client):
        def realpath(path):
            return '/real' + path
        mock_os.path.realpath = realpath
        mock_add_volume_binding = Mock()
        mock_add_emptydir_volume = Mock()
        mock_add_emptydir_volume_binding = Mock()
        mock_volume_builder.return_value.add_volume_binding = mock_add_volume_binding
        mock_volume_builder.return_value.add_emptydir_volume = mock_add_emptydir_volume
        mock_volume_builder.return_value.add_emptydir_volume_binding = mock_add_emptydir_volume_binding

        mock_pod_builder.return_value.build.return_value = '<built pod>'
        job = self.make_job()
        job.outdir = '/outdir'
        job.tmpdir = '/tmpdir'
        mock_runtime_context = Mock(tmpdir_prefix='TP', pod_serviceaccount=None)
        built = job.create_kubernetes_runtime(mock_runtime_context)
        # Adds volume binding for outdir
        self.assertEqual(mock_add_volume_binding.call_args, call('/real/outdir', '/out', True))
        # Adds emptydir binding for tmpdir
        self.assertEqual(mock_add_emptydir_volume.call_args, call('tmpdir'))
        self.assertEqual(mock_add_emptydir_volume_binding.call_args, call('tmpdir', '/tmp'))
        # looks at generatemapper
        # creates a KubernetesPodBuilder
        self.assertEqual(mock_pod_builder.call_args, call(
            job.name,
            job._get_container_image(),
            job.environment,
            job.volume_builder.volume_mounts,
            job.volume_builder.volumes,
            job.command_line,
            job.stdout,
            job.stderr,
            job.stdin,
            job.builder.resources,
            mock_read_yaml.return_value,
            mock_read_yaml.return_value,
            job.get_security_context(mock_runtime_context),
            None
        ))
        # calls builder.build
        # returns that
        self.assertTrue(mock_pod_builder.return_value.build.called)
        self.assertEqual(built, mock_pod_builder.return_value.build.return_value)

    def test_execute_kubernetes_pod(self, mock_volume_builder, mock_client):
        job = self.make_job()
        k8s_pod = Mock()
        job.execute_kubernetes_pod(k8s_pod)
        self.assertTrue(mock_client.return_value.submit_pod.called_with(k8s_pod))

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

    @patch('calrissian.job.quoted_arg_list')
    def test_quoted_command_line(self, mock_quoted_arg_list, mock_volume_builder, mock_client):
        job = self.make_job()
        result = job.quoted_command_line()
        self.assertEqual(mock_quoted_arg_list.return_value, result)
        self.assertEqual(mock_quoted_arg_list.call_args, call(job.command_line))

    def test_run(self, mock_volume_builder, mock_client):
        job = self.make_job()
        job.make_tmpdir = Mock()
        job.populate_env_vars = Mock()
        job._setup = Mock()
        job.create_kubernetes_runtime = Mock()
        job.execute_kubernetes_pod = Mock()
        job.wait_for_kubernetes_pod = Mock()
        job.report = Mock()
        job.finish = Mock()

        job.run(self.runtime_context)
        self.assertTrue(job.make_tmpdir.called)
        self.assertTrue(job.populate_env_vars.called)
        self.assertEqual(job._setup.call_args, call(self.runtime_context))
        self.assertEqual(job.create_kubernetes_runtime.call_args, call(self.runtime_context))
        self.assertEqual(job.execute_kubernetes_pod.call_args, call(job.create_kubernetes_runtime.return_value))
        self.assertTrue(job.wait_for_kubernetes_pod.called)
        self.assertEqual(job.finish.call_args, call(job.wait_for_kubernetes_pod.return_value, self.runtime_context))

    def test_run_uses_tmpdir_lock(self, mock_volume_builder, mock_client):
        mock_make_tmpdir = Mock()
        mock_enter = Mock()
        mock_exit = Mock()
        job = self.make_job()
        job.make_tmpdir = mock_make_tmpdir
        job.populate_env_vars = Mock()
        job._setup = Mock()
        job.create_kubernetes_runtime = Mock()
        job.execute_kubernetes_pod = Mock()
        job.wait_for_kubernetes_pod = Mock()
        job.finish = Mock()

        mock_tmpdir_lock = Mock(__enter__=mock_enter, __exit__=mock_exit)
        manager = Mock()
        manager.attach_mock(mock_enter, 'enter')
        manager.attach_mock(mock_exit, 'exit')
        manager.attach_mock(mock_make_tmpdir, 'make_tmpdir')
        job.run(self.runtime_context, mock_tmpdir_lock)
        expected_calls = [
            call.enter(),
            call.make_tmpdir(),
            call.exit(None, None, None)
            ]
        self.assertEqual(expected_calls, manager.mock_calls)

    def test_get_security_context(self, mock_volume_builder, mock_client):
        mock_runtime_context = Mock(no_match_user=False)
        expected_security_context = { 'runAsUser': os.getuid(), 'runAsGroup': os.getgid() }
        job = self.make_job()
        security_context = job.get_security_context(mock_runtime_context)
        self.assertEqual(security_context, expected_security_context)
    
    def test_get_security_context_empty(self, mock_volume_builder, mock_client):
        mock_runtime_context = Mock(no_match_user=True)
        job = self.make_job()
        security_context = job.get_security_context(mock_runtime_context)
        self.assertEqual(security_context, {})

    @patch('calrissian.job.read_yaml')
    def test_get_pod_labels(self, mock_read_yaml, mock_volume_builder, mock_client):
        expected_labels = {'foo':'bar'}
        mock_read_yaml.return_value = expected_labels
        mock_runtime_context = Mock(pod_labels='labels.yaml')
        job = self.make_job()
        labels = job.get_pod_labels(mock_runtime_context)
        self.assertEqual(labels, expected_labels)
        self.assertEqual(mock_read_yaml.call_args, call('labels.yaml'))

    def test_get_pod_labels_empty(self, mock_volume_builder, mock_client):
        mock_runtime_context = Mock(pod_labels=None)
        job = self.make_job()
        labels = job.get_pod_labels(mock_runtime_context)
        self.assertEqual(labels, {})
        
    @patch('calrissian.job.read_yaml')
    def test_get_pod_nodeselectors(self, mock_read_yaml, mock_volume_builder, mock_client):
        expected_nodeselectors = {'disktype':'ssd'}
        mock_read_yaml.return_value = expected_nodeselectors
        mock_runtime_context = Mock(pod_nodeselectors='nodeselectors.yaml')
        job = self.make_job()
        nodeselectors = job.get_pod_nodeselectors(mock_runtime_context)
        self.assertEqual(nodeselectors, expected_nodeselectors)
        self.assertEqual(mock_read_yaml.call_args, call('nodeselectors.yaml'))

    def test_get_pod_nodeselectors_empty(self, mock_volume_builder, mock_client):
        mock_runtime_context = Mock(pod_nodeselectors=None)
        job = self.make_job()
        nodeselectors = job.get_pod_nodeselectors(mock_runtime_context)
        self.assertEqual(nodeselectors, {})

    @patch('calrissian.job.read_yaml')
    def test_get_pod_env_vars(self, mock_read_yaml, mock_volume_builder, mock_client):
        expected_env_vars = {'HTTP_PROXY':'1.2.3.4:3853'}
        mock_read_yaml.return_value = expected_env_vars
        mock_runtime_context = Mock(pod_env_vars='env_vars.yaml')
        job = self.make_job()
        env_vars = job.get_pod_env_vars(mock_runtime_context)
        self.assertEqual(env_vars, expected_env_vars)
        self.assertEqual(mock_read_yaml.call_args, call('env_vars.yaml'))

    def test_get_env_vars_empty(self, mock_volume_builder, mock_client):
        mock_runtime_context = Mock(pod_env_vars=None)
        job = self.make_job()
        env_vars = job.get_pod_env_vars(mock_runtime_context)
        self.assertEqual(env_vars, {})

    def test_get_from_requirements_raises_not_implemented(self,  mock_volume_builder, mock_client):
        job = self.make_job()
        with self.assertRaisesRegex(NotImplementedError, 'get_from_requirements'):
            job.get_from_requirements(Mock(), Mock())

    def test_create_runtime_raises_not_implemented(self,  mock_volume_builder, mock_client):
        job = self.make_job()
        with self.assertRaisesRegex(NotImplementedError, 'create_runtime'):
            job.create_runtime(Mock(), Mock())

    @patch('calrissian.job.CalrissianCommandLineJob._add_volume_binding')
    def test_append_volume(self, mock_add_volume_binding, mock_volume_builder, mock_client):
        job = self.make_job()
        job.append_volume([], 'source', 'target', True)
        self.assertEqual(mock_add_volume_binding.call_args, call('source','target',True))


class TotalSizeTestCase(TestCase):

    def make_file(self, size=None, path=None):
        file = {'class':'File'}
        if size:
            file['size'] = size
        if path:
            file['path'] = path
        return file


    def test_total_size_direct(self):
        file = self.make_file(100)
        self.assertEqual(total_size(file), 100)

    def test_total_size_file_objects(self):
        outputs = {
            'file1': self.make_file(100, 'file1'),
            'file2': self.make_file(200, 'file2')
        }
        self.assertEqual(total_size(outputs), 300)

    def test_defaults_zero_when_no_size(self):
        file = self.make_file()
        self.assertNotIn('size', file)
        self.assertEqual(total_size(file), 0)


    def test_counts_in_array(self):
        outputs = {
            'files': [
                self.make_file(100),
                self.make_file(200),
                self.make_file(300)
            ]
        }
        self.assertEqual(total_size(outputs), 600)

    def test_counts_nested(self):
        outputs = {
            'files': [
                self.make_file(100),
                self.make_file(200),
                self.make_file(300)
            ],
            'things': {
                'nested_files': [
                    self.make_file(1000),
                    self.make_file(2000),
                    self.make_file(3000)
                ]
            }
        }
        self.assertEqual(total_size(outputs), 6600)

    def test_counts_secondary_files(self):
        file = self.make_file(1000)
        file['secondaryFiles'] = [
            self.make_file(100),
            self.make_file(200),
            self.make_file(300)
        ]
        self.assertEqual(total_size(file), 1600)
