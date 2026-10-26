import os
from types import SimpleNamespace
import threading
from unittest import TestCase
from unittest.mock import PropertyMock, create_autospec, patch, call, Mock, mock_open

from cwltool.utils import CWLObjectType
from calrissian.context import CalrissianRuntimeContext
from calrissian.executor import IncompleteStatusException
from calrissian.job import DEFAULT_INIT_IMAGE
from calrissian.dask import (
    CalrissianCommandLineDaskJob,
    KubernetesDaskPodBuilder,
    KubernetesDaskClient
)

from calrissian.dask import (
    dask_req_validate,
)
from calrissian.k8s import (
    CompletionResult,
)
from kubernetes.client.models import V1Pod, V1Container, V1ContainerStatus


class ValidateExtensionTestCase(TestCase):

    def setUp(self):
        self.daskGatewayRequirement: CWLObjectType = {
            "workerCores": 2,
            "workerCoresLimit": 2,
            "workerMemory": "4G",
            "clusterMaxCores": 8,
            "clusterMaxMemory": "16G",
            "class": "https://calrissian-cwl.github.io/schema#DaskGatewayRequirement" # From cwl
        }
    
    def test_validate_extension(self):
        self.assertTrue(dask_req_validate(self.daskGatewayRequirement))


class KubernetesDaskPodBuilderTestCase(TestCase):

    def setUp(self):
        builder = Mock()
        builder.cwlVersion = "v1.2"
        builder.requirements = []
        builder.resources = {'cores': 1, 'ram': 1024}
        self.name = 'PodName'
        self.builder = builder
        self.container_image = 'dockerimage:1.0'
        self.volume_mounts = [Mock(), Mock()]
        self.volumes = [Mock()]
        self.command_line = ['cat']
        self.stdout = 'stdout.txt'
        self.stderr = 'stderr.txt'
        self.stdin = 'stdin.txt'
        self.labels = {'key1': 'val1', 'key2': 123}
        self.nodeselectors = {'disktype': 'ssd', 'cachelevel': 2}
        self.gpu_nodeselectors = {}
        self.security_context = { 'runAsUser': os.getuid(),'runAsGroup': os.getgid() }
        self.pod_serviceaccount = "podmanager"
        self.dask_gateway_url = "http://dask-gateway-url:80"
        self.dask_gateway_controller = False
        self.environment = {
            'HOME': '/homedir',
            'PYTHONPATH': '/app',
        }
        self.no_network_access_pod_labels = {}
        self.network_access_pod_labels = {}
        self.pod_additional_spec = {}
        self.pod_builder = KubernetesDaskPodBuilder(self.dask_gateway_url, self.dask_gateway_controller, self.name, self.builder, self.container_image,
                                                    self.environment, self.volume_mounts, self.volumes, self.command_line, self.stdout, self.stderr,
                                                    self.stdin, self.labels, self.nodeselectors, self.gpu_nodeselectors, self.security_context, self.pod_serviceaccount,
                                                    self.pod_additional_spec, self.no_network_access_pod_labels, self.network_access_pod_labels )
        self.pod_builder.dask_requirement = {
            "workerCores": 2,
            "workerCoresLimit": 2,
            "workerMemory": "4G",
            "clusterMaxCores": 8,
            "clusterMaxMemory": "16G",
            "class": "https://calrissian-cwl.github.io/schema#DaskGatewayRequirement" # From cwl
        }

    def test_main_container_args_without_redirects(self):
        # container_args returns a list with a single item since it is passed to 'sh', '-c'
        self.pod_builder.stdout = None
        self.pod_builder.stderr = None
        self.pod_builder.stdin = None
        self.assertEqual(['set -e; trap "touch /shared/completed" EXIT;export DASK_CLUSTER=$(cat /shared/dask_cluster_name.txt) ; cat' ], self.pod_builder.container_args())

    
    def test_container_args_with_redirects(self):
        self.assertEqual(['set -e; trap "touch /shared/completed" EXIT;export DASK_CLUSTER=$(cat /shared/dask_cluster_name.txt) ; cat > stdout.txt 2> stderr.txt < stdin.txt'], self.pod_builder.container_args())


    def test_init_container_command_with_external_script(self):
        self.pod_builder.dask_gateway_controller = True
        self.assertEqual(['python', '/controller/init-dask.py'], self.pod_builder.init_container_command())


    @patch("builtins.open", new_callable=mock_open, read_data="print('Default script')")
    @patch("os.path.join", return_value="/mocked/path/init-dask.py")  # Mock path join
    def test_init_container_command_with_default_script(self, mock_path_join, mock_file):
        self.pod_builder.dask_gateway_controller = False
        expected_command = ['python', '-c', "print('Default script')"]
        self.assertEqual(expected_command, self.pod_builder.init_container_command())


    def test_sidecar_container_command_with_external_script(self):
        self.pod_builder.dask_gateway_controller = True
        self.assertEqual(['python', '/controller/dispose-dask.py'], self.pod_builder.sidecar_container_command())


    @patch("builtins.open", new_callable=mock_open, read_data="print('Default script')")
    @patch("os.path.join", return_value="/mocked/path/dispose-dask.py")  # Mock path join
    def test_sidecar_container_command_with_default_script(self, mock_path_join, mock_file):
        self.pod_builder.dask_gateway_controller = False
        expected_command = ['python', '-c', "print('Default script')"]
        self.assertEqual(expected_command, self.pod_builder.sidecar_container_command())


    def test_container_environment(self):
        environment = self.pod_builder.container_environment()
        self.assertEqual(len(self.environment) + 8, len(environment)) # +8 Because the dask related are added at runtime 
        self.assertIn({'name': 'HOME', 'value': '/homedir'}, environment)
        self.assertIn({'name': 'PYTHONPATH', 'value': '/app'}, environment)
        self.assertIn({'name': 'DASK_GATEWAY_WORKER_CORES', 'value': '2'}, environment)
        self.assertIn({'name': 'DASK_GATEWAY_WORKER_MEMORY', 'value': '4G'}, environment)
        self.assertIn({'name': 'DASK_GATEWAY_WORKER_CORES_LIMIT', 'value': '2'}, environment)
        self.assertIn({'name': 'DASK_GATEWAY_CLUSTER_MAX_CORES', 'value': '8'}, environment)
        self.assertIn({'name': 'DASK_GATEWAY_CLUSTER_MAX_RAM', 'value': '16G'}, environment)
        self.assertIn({'name': 'DASK_GATEWAY_URL', 'value': 'http://dask-gateway-url:80'}, environment)
        self.assertIn({'name': 'DASK_GATEWAY_IMAGE', 'value': 'dockerimage:1.0'}, environment)  # Replace with actual image if needed
        self.assertIn({'name': 'DASK_CLUSTER_NAME_PATH', 'value': '/shared/dask_cluster_name.txt'}, environment)


    def test_init_containers_when_no_stdout_or_stderr(self):
        self.pod_builder.stdout = None
        self.pod_builder.stderr = None
        self.assertEqual(len(self.pod_builder.init_containers()), 1)

    def test_init_containers_when_stdout_is_local_file(self):
        self.pod_builder.stdout = 'stdout.txt'
        self.pod_builder.stderr = None
        self.assertEqual(len(self.pod_builder.init_containers()), 1)

    def test_init_containers_when_stderr_is_local_file(self):
        self.pod_builder.stdout = None
        self.pod_builder.stderr = 'stderr.txt'
        self.assertEqual(len(self.pod_builder.init_containers()), 1)

    def test_init_containers_when_stdout_has_path(self):
        self.pod_builder.stdout = 'out/to/stdout.txt'
        self.pod_builder.stderr = 'err/to/stderr.txt'
        init_containers = self.pod_builder.init_containers()
        self.assertEqual(len(init_containers), 2)
        container = init_containers[0]
        self.assertEqual(container['name'], 'podname-init')
        self.assertEqual(container['image'], DEFAULT_INIT_IMAGE)
        self.assertEqual(container['command'], ['/bin/sh','-c','mkdir -p out/to; mkdir -p err/to;'])
        self.assertEqual(container['volumeMounts'], self.pod_builder.volume_mounts)
    

    @patch("builtins.open", new_callable=mock_open, read_data="print('Default script')")
    @patch("os.path.join", return_value="/mocked/path/dispose-dask.py")  # Mock path join
    def test_dask_init_container(self, mock_path_join, mock_file):
        self.pod_builder.stdout = None
        self.pod_builder.stderr = None
        init_containers = self.pod_builder.init_containers()
        self.assertEqual(len(init_containers), 1)
        container = init_containers[0]

        self.assertEqual(container['env'], self.pod_builder.container_environment())

        expected_command = ['python', '-c', "print('Default script')"]
        self.assertEqual(container['command'], expected_command)
        self.assertEqual(container['image'], self.container_image)

    @patch("builtins.open", new_callable=mock_open, read_data="print('Default script')")
    @patch("os.path.join", return_value="/mocked/path/dispose-dask.py")  # Mock path join
    def test_dask_init_containers_when_stdout_has_path(self, mock_path_join, mock_file):
        self.pod_builder.stdout = 'out/to/stdout.txt'
        self.pod_builder.stderr = 'err/to/stderr.txt'
        init_containers = self.pod_builder.init_containers()
        self.assertEqual(len(init_containers), 2)
        container = init_containers[0]
        dask_container = init_containers[1]

        self.assertEqual(container['name'], 'podname-init')
        self.assertEqual(container['image'], DEFAULT_INIT_IMAGE)
        self.assertEqual(container['command'], ['/bin/sh','-c','mkdir -p out/to; mkdir -p err/to;'])
        self.assertEqual(container['volumeMounts'], self.pod_builder.volume_mounts)

        self.assertEqual(dask_container['env'], self.pod_builder.container_environment())
        expected_command = ['python', '-c', "print('Default script')"]
        self.assertEqual(dask_container['command'], expected_command)
        self.assertEqual(dask_container['image'], self.container_image)

    
    def test_dask_init_container_custom_command(self):
        self.pod_builder.stdout = None
        self.pod_builder.stderr = None
        self.pod_builder.dask_gateway_controller = True
        init_containers = self.pod_builder.init_containers()
        self.assertEqual(len(init_containers), 1)
        container = init_containers[0]

        expected_command = ['python', '/controller/init-dask.py']
        self.assertEqual(container['command'], expected_command)


    @patch("builtins.open", new_callable=mock_open, read_data="print('Default script')")
    @patch("os.path.join", return_value="/mocked/path/file-dask.py")
    @patch('calrissian.job.random_tag')
    def test_dask_build(self, mock_random_tag, mock_path_join, mock_file):
        mock_random_tag.return_value = 'random'
        expected = {
            'metadata': {
                'name': 'podname-pod-random',
                'labels': {
                    'key1': 'val1',
                    'key2': '123'
                }
            },
            'apiVersion': 'v1',
            'kind': 'Pod',
            'spec': {
                'initContainers': [
                    {
                        'name': 'podname-init',
                        'image': 'dockerimage:1.0',
                        'env': [
                            {'name': 'HOME', 'value': '/homedir'},
                            {'name': 'PYTHONPATH', 'value': '/app'},
                            {'name': 'DASK_GATEWAY_WORKER_CORES', 'value': '2'},
                            {'name': 'DASK_GATEWAY_WORKER_MEMORY', 'value': '4G'},
                            {'name': 'DASK_GATEWAY_WORKER_CORES_LIMIT', 'value': '2'}, 
                            {'name': 'DASK_GATEWAY_CLUSTER_MAX_CORES', 'value': '8'},
                            {'name': 'DASK_GATEWAY_CLUSTER_MAX_RAM', 'value': '16G'},
                            {'name': 'DASK_GATEWAY_URL', 'value': 'http://dask-gateway-url:80'},
                            {'name': 'DASK_GATEWAY_IMAGE', 'value': 'dockerimage:1.0'},
                            {'name': 'DASK_CLUSTER_NAME_PATH', 'value': '/shared/dask_cluster_name.txt'}
                        ],
                        'command': ['python', '-c', "print('Default script')"],
                        'workingDir': '/homedir',
                        'volumeMounts': self.volume_mounts,
                    }
                ],
                'containers': [
                    {
                        'name': 'main-container',
                        'image': 'dockerimage:1.0',
                        'command': ['/bin/sh', '-c'],
                        'args': ['set -e; trap "touch /shared/completed" EXIT;export DASK_CLUSTER=$(cat /shared/dask_cluster_name.txt) ; cat > stdout.txt 2> stderr.txt < stdin.txt'],
                        'env': [
                            {'name': 'HOME', 'value': '/homedir'},
                            {'name': 'PYTHONPATH', 'value': '/app'},
                            {'name': 'DASK_GATEWAY_WORKER_CORES', 'value': '2'},
                            {'name': 'DASK_GATEWAY_WORKER_MEMORY', 'value': '4G'},
                            {'name': 'DASK_GATEWAY_WORKER_CORES_LIMIT', 'value': '2'}, 
                            {'name': 'DASK_GATEWAY_CLUSTER_MAX_CORES', 'value': '8'},
                            {'name': 'DASK_GATEWAY_CLUSTER_MAX_RAM', 'value': '16G'},
                            {'name': 'DASK_GATEWAY_URL', 'value': 'http://dask-gateway-url:80'},
                            {'name': 'DASK_GATEWAY_IMAGE', 'value': 'dockerimage:1.0'},
                            {'name': 'DASK_CLUSTER_NAME_PATH', 'value': '/shared/dask_cluster_name.txt'}
                        ],
                        'resources': {
                            'requests': {
                                'cpu': '1',
                                'memory': '1024Mi'
                            }
                        },
                        'volumeMounts': self.volume_mounts,
                        'workingDir': '/homedir',
                    },
                    {
                        'name': 'sidecar-container',
                        'image': 'dockerimage:1.0',
                        'command': ['python', '-c', "print('Default script')"],
                        'env': [
                            {'name': 'HOME', 'value': '/homedir'},
                            {'name': 'PYTHONPATH', 'value': '/app'},
                            {'name': 'DASK_GATEWAY_WORKER_CORES', 'value': '2'},
                            {'name': 'DASK_GATEWAY_WORKER_MEMORY', 'value': '4G'},
                            {'name': 'DASK_GATEWAY_WORKER_CORES_LIMIT', 'value': '2'}, 
                            {'name': 'DASK_GATEWAY_CLUSTER_MAX_CORES', 'value': '8'},
                            {'name': 'DASK_GATEWAY_CLUSTER_MAX_RAM', 'value': '16G'},
                            {'name': 'DASK_GATEWAY_URL', 'value': 'http://dask-gateway-url:80'},
                            {'name': 'DASK_GATEWAY_IMAGE', 'value': 'dockerimage:1.0'},
                            {'name': 'DASK_CLUSTER_NAME_PATH', 'value': '/shared/dask_cluster_name.txt'}
                        ],
                        'resources': {
                            'requests': {
                                'cpu': '1',
                                'memory': '1024Mi'
                            }
                        },
                        'volumeMounts': self.volume_mounts,
                        'workingDir': '/homedir',
                    }
                ],
                'restartPolicy': 'Never',
                'volumes': self.volumes,
                'securityContext': {
                    'runAsUser': os.getuid(),
                    'runAsGroup': os.getgid()
                },
                'nodeSelector': {
                    "disktype": "ssd",
                    "cachelevel": "2"
                },
                'serviceAccountName': 'podmanager'
            }
        }

        self.assertEqual(expected, self.pod_builder.build())

        # test build with custom script
        self.pod_builder.dask_gateway_controller = True
        expected['spec']['initContainers'][0]['command'] = ['python', '/controller/init-dask.py']
        expected['spec']['containers'][1]['command'] = ['python', '/controller/dispose-dask.py']
        self.assertEqual(expected, self.pod_builder.build())


@patch('calrissian.dask.KubernetesDaskClient')
@patch('calrissian.job.KubernetesVolumeBuilder')
class CalrissianCommandLineDaskJobTestCase(TestCase):

    def setUp(self):
        self.builder = Mock(outdir='/out')
        self.joborder = Mock()
        self.make_path_mapper = Mock()
        self.requirements = [{'class': 'DockerRequirement', 'dockerPull': 'dockerimage:1.0'}]
        self.hints = []
        self.name = 'test-cldj' # test-commandLineDaskJob
        self.runtime_context = CalrissianRuntimeContext(
            {'workflow_eval_lock': threading.Lock(),
             'dask_gateway_url': 'dask_gateway_url'})

    @patch('calrissian.k8s.load_config_get_namespace', return_value='default')
    @patch('calrissian.k8s.KubernetesClient.get_current_pod', return_value=Mock())
    # Since the mock_client is the KubernetesDaskClient, that extends KubernetesClient,
    # I need to mock also the get_current_pod from the superclass
    def make_job(self, mock_pod, mock_ns):

        job = CalrissianCommandLineDaskJob(
            self.builder,
            self.joborder,
            self.make_path_mapper,
            self.requirements,
            self.hints,
            self.name
        )
        mock_collected_outputs = Mock()
        job.collect_outputs = Mock()
        job.collect_outputs.return_value = mock_collected_outputs
        job.output_callback = Mock()
        return job
    
    def make_completion_result(self, exit_code):
        return create_autospec(CompletionResult, pod_name=self.name, exit_code=exit_code, cpus='1', memory='1', start_time=Mock(),
                        finish_time=Mock(), pod_log='logs/')

    
    def test_wait_for_kubernetes_dask_pod(self, mock_volume_builder, mock_client):
        job = self.make_job()
        job.wait_for_kubernetes_pod(cm_name='dask-cm-random')
        
        self.assertTrue(mock_client.return_value.wait_for_completion.called)  


    def test_get_dask_gateway_url(self, mock_volume_builder, mock_client):
        job = self.make_job()
        dask_gateway_url = job.get_dask_gateway_url(self.runtime_context)
        self.assertEqual(dask_gateway_url, 'dask_gateway_url')


    def test_dask_configmap_name(self, mock_volume_builder, mock_client):
        job = self.make_job()
        self.assertRegex(job.dask_cm_name, r'^dask-cm-[a-zA-Z0-9]{8}$')
    
    def test_add_configmap_volume_and_binding(self, mock_volume_builder, mock_client):
        job = self.make_job()
        job._add_configmap_volume_and_binding(
            job.dask_cm_name, job.dask_cm_name, job.dask_gateway_config_dir
        )
        self.assertTrue(mock_volume_builder.return_value.add_configmap_volume.called)
        self.assertTrue(mock_volume_builder.return_value.add_configmap_volume_binding.called)


    @patch('calrissian.dask.KubernetesDaskPodBuilder')
    @patch('calrissian.dask.os')
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
        mock_runtime_context = Mock(tmpdir_prefix='TP', pod_serviceaccount=None, )
        built = job.create_kubernetes_runtime(mock_runtime_context)
        # Adds volume binding for outdir
        self.assertEqual(mock_add_volume_binding.call_args, call('/real/outdir', '/out', True))
        # Adds emptydir binding for shared-data
        self.assertEqual(mock_add_emptydir_volume.call_args, call('shared-data'))
        self.assertEqual(mock_add_emptydir_volume_binding.call_args, call('shared-data', '/shared'))
        # looks at generatemapper
        # creates a KubernetesPodBuilder
        self.assertEqual(mock_pod_builder.call_args, call(
            job.get_dask_gateway_url(mock_runtime_context),
            job.client.get_configmap_from_namespace(mock_runtime_context),
            job.name,
            job.builder,
            job._get_container_image(),
            job.environment,
            job.volume_builder.volume_mounts,
            job.volume_builder.volumes,
            job.command_line,
            job.stdout,
            job.stderr,
            job.stdin,
            mock_read_yaml.return_value,
            mock_read_yaml.return_value,
            mock_read_yaml.return_value,
            job.get_security_context(mock_runtime_context),
            None,
            job.get_pod_additional_spec(mock_runtime_context),
            mock_read_yaml.return_value,
            mock_read_yaml.return_value
        ))
        # calls builder.build
        # returns that
        self.assertTrue(mock_pod_builder.return_value.build.called)
        self.assertEqual(built, mock_pod_builder.return_value.build.return_value)

    
    @patch('calrissian.dask.KubernetesDaskPodBuilder')
    def test_run(self, mock_pod_builder, mock_volume_builder, mock_client):
        job = self.make_job()
        job.make_tmpdir = Mock()
        job.populate_env_vars = Mock()
        job._setup = Mock()
        job.create_kubernetes_runtime = Mock()
        job.execute_kubernetes_pod = Mock()
        job.wait_for_kubernetes_pod = Mock()
        job.report = Mock()
        job.finish = Mock()
        job.create_kubernetes_runtime = mock_pod_builder
        
        job.run(self.runtime_context)
        self.assertTrue(job.make_tmpdir.called)
        self.assertTrue(job.populate_env_vars.called)
        self.assertEqual(job._setup.call_args, call(self.runtime_context))
        self.assertEqual(job.create_kubernetes_runtime.call_args, call(self.runtime_context))
        self.assertEqual(job.execute_kubernetes_pod.call_args, call(job.create_kubernetes_runtime.return_value))
        self.assertTrue(job.wait_for_kubernetes_pod.called)
        self.assertEqual(job.finish.call_args, call(job.wait_for_kubernetes_pod.return_value, self.runtime_context))


@patch('calrissian.k8s.client', autospec=True)
@patch('calrissian.k8s.load_config_get_namespace', autospec=True)
class KubernetesDaskClientTestCase(TestCase):

    def test_init(self, mock_get_namespace, mock_client):
        kc = KubernetesDaskClient()
        self.assertEqual(kc.namespace, mock_get_namespace.return_value)
        self.assertEqual(kc.core_api_instance, mock_client.CoreV1Api.return_value)
        self.assertIsNone(kc.pod)
        self.assertIsNone(kc.completion_result)


    @patch('calrissian.dask.DaskPodMonitor')
    def test_submit_pod(self, mock_podmonitor, mock_get_namespace, mock_client):
        mock_get_namespace.return_value = 'namespace'
        mock_create_namespaced_pod = Mock()
        mock_create_namespaced_pod.return_value = Mock(metadata=Mock(uid='123'))
        mock_client.CoreV1Api.return_value.create_namespaced_pod = mock_create_namespaced_pod
        kc = KubernetesDaskClient()
        mock_body = Mock()
        kc.submit_pod(mock_body)
        self.assertEqual(kc.pod.metadata.uid, '123')
        self.assertEqual(mock_create_namespaced_pod.call_args, call('namespace', mock_body))
        # This is to inspect `with PodMonitor() as monitor`:
        self.assertTrue(mock_podmonitor.return_value.__enter__.return_value.add.called)


    def setup_mock_watch(self, mock_watch, event_objects=[]):
        mock_stream = Mock()
        mock_stop = Mock()
        stream_events = []
        for event_object in event_objects:
            stream_events.append({'object': event_object})
        mock_stream.return_value = stream_events
        mock_watch.Watch.return_value.stream = mock_stream
        mock_watch.Watch.return_value.stop = mock_stop

    def make_mock_pod(self, name):
        mock_metadata = Mock()
        type(mock_metadata).name = PropertyMock(return_value=name)
        mock_pod = create_autospec(V1Pod, metadata=mock_metadata)
        return mock_pod

    def mock_k8s_obj_with_name(self, cls, name: str):
        obj = create_autospec(cls, instance=True)
        type(obj).name = PropertyMock(return_value=name)
        return obj

    def make_mock_container(self, name="main-container"):
        # Make something that behaves like a k8s container for _extract_cpu_memory_requests
        # It expects container.resources.requests to exist.
        return SimpleNamespace(
            name=name,
            resources=SimpleNamespace(
                requests={"cpu": "1", "memory": "1Mi"}
            )
        )

    
    @patch('calrissian.dask.watch', autospec=True)
    def test_wait_calls_watch_pod_with_pod_name_field_selector(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = self.make_mock_pod('test123')

        mock_pod.status = Mock()
        mock_pod.status.init_container_statuses = None
        mock_pod.status.container_statuses = [Mock()]

        mock_pod.spec = Mock()
        mock_pod.spec.containers = [self.make_mock_container("main-container")]

        mock_pod.status.container_statuses[0].state = Mock(running=None, waiting=None, terminated=Mock(exit_code=0))
        
        self.setup_mock_watch(mock_watch, [mock_pod])

        kc = KubernetesDaskClient()
        kc._set_pod(mock_pod)
        kc.wait_for_completion(cm_name='dask-cm-random')
        mock_stream = mock_watch.Watch.return_value.stream
        self.assertEqual(mock_stream.call_args, call(kc.core_api_instance.list_namespaced_pod, kc.namespace,
                                                     field_selector='metadata.name=test123'))


    @patch('calrissian.dask.watch', autospec=True)
    def test_wait_calls_watch_pod_with_incomplete_status(self, mock_watch, mock_get_namespace, mock_client):
        self.setup_mock_watch(mock_watch)
        mock_pod = self.make_mock_pod('test123')
        kc = KubernetesDaskClient()
        kc._set_pod(mock_pod)
        # Assert IncompleteStatusException is raised
        with self.assertRaises(IncompleteStatusException):
            kc.wait_for_completion(cm_name='dask-cm-random')
    
    @patch('calrissian.dask.watch', autospec=True)
    def test_wait_skips_pod_when_containers_status_is_none(self, mock_watch, mock_get_namespace, mock_client):
    
        mock_pod = self.make_mock_pod('test123')
        mock_pod.status.container_statuses = None

        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesDaskClient()
        kc._set_pod(Mock())
        with self.assertRaises(IncompleteStatusException):
            kc.wait_for_completion(cm_name='dask-cm-random')
        self.assertFalse(mock_watch.Watch.return_value.stop.called)
        self.assertFalse(mock_client.CoreV1Api.return_value.delete_namespaced_pod.called)
        self.assertIsNotNone(kc.pod)


    @patch('calrissian.dask.watch', autospec=True)
    def test_wait_skips_pod_when_state_is_waiting(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = create_autospec(V1Pod)
        mock_pod.status.container_statuses[0].state = Mock(running=None, waiting=True, terminated=None)
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesDaskClient()
        kc._set_pod(Mock())
        with self.assertRaises(IncompleteStatusException):
            kc.wait_for_completion(cm_name='dask-cm-random')
        self.assertFalse(mock_watch.Watch.return_value.stop.called)
        self.assertFalse(mock_client.CoreV1Api.return_value.delete_namespaced_pod.called)
        self.assertIsNotNone(kc.pod)


    @patch('calrissian.dask.watch', autospec=True)
    @patch('calrissian.dask.DaskPodMonitor')
    @patch('calrissian.k8s.KubernetesClient._extract_cpu_memory_requests')
    def test_wait_finishes_when_pod_state_is_terminated(self, mock_cpu_memory,
                                                        mock_podmonitor, mock_watch, mock_get_namespace,
                                                        mock_client):
        mock_pod = create_autospec(V1Pod)
        mock_pod.status = Mock()
        mock_pod.status.init_container_statuses = None
        mock_pod.status.container_statuses = [Mock()]

        mock_pod.spec = Mock()
        mock_pod.spec.containers = [self.make_mock_container("main-container")]
        mock_pod.status.container_statuses[0].state = Mock(running=None, waiting=None, terminated=Mock(exit_code=123))
        mock_cpu_memory.return_value = ('1', '1Mi')
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesDaskClient()
        kc._set_pod(Mock())
        completion_result = kc.wait_for_completion(cm_name='dask-cm-random')
        self.assertEqual(completion_result.exit_code, 123)
        self.assertTrue(mock_watch.Watch.return_value.stop.called)
        self.assertTrue(mock_client.CoreV1Api.return_value.delete_namespaced_pod.called)
        self.assertIsNone(kc.pod)
        # This is to inspect `with PodMonitor() as monitor`:
        self.assertTrue(mock_podmonitor.return_value.__enter__.return_value.remove.called)
    

    def test_get_container_by_name_returns_none_when_list_is_none(self, mock_get_namespace, mock_client):
        res = KubernetesDaskClient.get_container_by_name(None, "whatever")
        self.assertIsNone(res)


    def test_get_container_by_name_returns_none_when_list_is_empty(self, mock_get_namespace, mock_client):
        res = KubernetesDaskClient.get_container_by_name([], "whatever")
        self.assertIsNone(res)


    def test_get_container_by_name_returns_matching_v1container(self, mock_get_namespace, mock_client):
        c1 = self.mock_k8s_obj_with_name(V1Container, "a")
        c2 = self.mock_k8s_obj_with_name(V1Container, "target")
        c3 = self.mock_k8s_obj_with_name(V1Container, "b")

        res = KubernetesDaskClient.get_container_by_name([c1, c2, c3], "target")
        self.assertIs(res, c2)


    def test_get_container_by_name_returns_matching_v1containerstatus(self, mock_get_namespace, mock_client):
        s1 = self.mock_k8s_obj_with_name(V1ContainerStatus, "x")
        s2 = self.mock_k8s_obj_with_name(V1ContainerStatus, "target")

        res = KubernetesDaskClient.get_container_by_name([s1, s2], "target")
        self.assertIs(res, s2)


    def test_get_container_by_name_returns_none_when_not_found(self, mock_get_namespace, mock_client):
        c1 = self.mock_k8s_obj_with_name(V1Container, "a")
        c2 = self.mock_k8s_obj_with_name(V1ContainerStatus, "b")

        res = KubernetesDaskClient.get_container_by_name([c1, c2], "missing")
        self.assertIsNone(res)


    def test_get_container_by_name_returns_first_match_if_duplicates(self, mock_get_namespace, mock_client):
        first = self.mock_k8s_obj_with_name(V1Container, "dup")
        second = self.mock_k8s_obj_with_name(V1ContainerStatus, "dup")

        res = KubernetesDaskClient.get_container_by_name([first, second], "dup")
        self.assertIs(res, first)
