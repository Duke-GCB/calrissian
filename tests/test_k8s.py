from unittest import TestCase
from unittest.mock import Mock, patch, call, PropertyMock, create_autospec
from kubernetes.client.models import V1Pod, V1ContainerStateTerminated, V1ContainerState
from kubernetes.client.api_client import ApiException
from kubernetes.config.config_exception import ConfigException
from calrissian.executor import IncompleteStatusException
from calrissian.k8s import load_config_get_namespace, KubernetesClient, CalrissianJobException, PodMonitor, delete_pods
from calrissian.k8s import CompletionResult, read_file


class ReadFileTestCase(TestCase):

    @patch('builtins.open')
    def test_read_file(self, mock_open):
        mock_result = Mock()
        mock_open.return_value.__enter__.return_value.read.return_value = mock_result
        result = read_file('filename.txt')
        self.assertEqual(result, mock_result)
        self.assertEqual(mock_open.call_args, call('filename.txt'))


@patch('calrissian.k8s.read_file')
@patch('calrissian.k8s.config')
class LoadConfigTestCase(TestCase):

    def test_load_config_get_namespace_incluster(self, mock_config, mock_read_file):
        mock_read_file.return_value = 'in-cluster-namespace'
        namespace = load_config_get_namespace()
        self.assertEqual(namespace, 'in-cluster-namespace')
        self.assertTrue(mock_read_file.called)
        self.assertTrue(mock_config.load_incluster_config.called)
        self.assertFalse(mock_config.load_kube_config.called)

    def test_load_config_get_namespace_external(self, mock_config, mock_read_file):
        # When load_incluster_config raises an exception, call load_kube_config and assume 'default'
        mock_config.load_incluster_config.side_effect = ConfigException
        namespace = load_config_get_namespace()
        self.assertEqual(namespace, 'default')
        self.assertFalse(mock_read_file.called)
        self.assertTrue(mock_config.load_incluster_config.called)
        self.assertTrue(mock_config.load_kube_config.called)


@patch('calrissian.k8s.client', autospec=True)
@patch('calrissian.k8s.load_config_get_namespace', autospec=True)
class KubernetesClientTestCase(TestCase):

    def test_init(self, mock_get_namespace, mock_client):
        kc = KubernetesClient()
        self.assertEqual(kc.namespace, mock_get_namespace.return_value)
        self.assertEqual(kc.core_api_instance, mock_client.CoreV1Api.return_value)
        self.assertIsNone(kc.pod)
        self.assertIsNone(kc.completion_result)

    @patch('calrissian.k8s.PodMonitor')
    def test_submit_pod(self, mock_podmonitor, mock_get_namespace, mock_client):
        mock_get_namespace.return_value = 'namespace'
        mock_create_namespaced_pod = Mock()
        mock_create_namespaced_pod.return_value = Mock(metadata=Mock(uid='123'))
        mock_client.CoreV1Api.return_value.create_namespaced_pod = mock_create_namespaced_pod
        kc = KubernetesClient()
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
        # Cannot mock name attribute without a propertymock
        name_property = PropertyMock(return_value=name)
        type(mock_metadata).name = name_property
        mock_pod = create_autospec(V1Pod, metadata=mock_metadata)
        return mock_pod

    @patch('calrissian.k8s.watch', autospec=True)
    def test_wait_calls_watch_pod_with_pod_name_field_selector(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = self.make_mock_pod('test123')
        mock_pod.status.container_statuses[0].state = Mock(running=None, waiting=None, terminated=Mock(exit_code=0))
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_pod(mock_pod)
        kc.wait_for_completion()
        mock_stream = mock_watch.Watch.return_value.stream
        self.assertEqual(mock_stream.call_args, call(kc.core_api_instance.list_namespaced_pod, kc.namespace,
                                                     field_selector='metadata.name=test123'))

    @patch('calrissian.k8s.watch', autospec=True)
    def test_wait_calls_watch_pod_with_imcomplete_status(self, mock_watch, mock_get_namespace, mock_client):
        self.setup_mock_watch(mock_watch)
        mock_pod = self.make_mock_pod('test123')
        kc = KubernetesClient()
        kc._set_pod(mock_pod)
        # Assert IncompleteStatusException is raised
        with self.assertRaises(IncompleteStatusException):
            kc.wait_for_completion()

    @patch('calrissian.k8s.watch', autospec=True)
    def test_wait_skips_pod_when_status_is_none(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = Mock(status=Mock(container_statuses=None))
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_pod(Mock())
        with self.assertRaises(IncompleteStatusException):
            kc.wait_for_completion()
        self.assertFalse(mock_watch.Watch.return_value.stop.called)
        self.assertFalse(mock_client.CoreV1Api.return_value.delete_namespaced_pod.called)
        self.assertIsNotNone(kc.pod)

    @patch('calrissian.k8s.watch', autospec=True)
    def test_wait_skips_pod_when_state_is_waiting(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = create_autospec(V1Pod)
        mock_pod.status.container_statuses[0].state = Mock(running=None, waiting=True, terminated=None)
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_pod(Mock())
        with self.assertRaises(IncompleteStatusException):
            kc.wait_for_completion()
        self.assertFalse(mock_watch.Watch.return_value.stop.called)
        self.assertFalse(mock_client.CoreV1Api.return_value.delete_namespaced_pod.called)
        self.assertIsNotNone(kc.pod)

    @patch('calrissian.k8s.watch', autospec=True)
    @patch('calrissian.k8s.KubernetesClient.follow_logs')
    def test_wait_follows_logs_pod_when_state_is_running(self, mock_follow_logs, mock_watch, mock_get_namespace, mock_client):
        mock_pod = create_autospec(V1Pod)
        mock_pod.status.container_statuses[0].state = Mock(running=True, waiting=None, terminated=None)
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_pod(Mock())
        with self.assertRaises(IncompleteStatusException):
            kc.wait_for_completion()
        self.assertFalse(mock_watch.Watch.return_value.stop.called)
        self.assertFalse(mock_client.CoreV1Api.return_value.delete_namespaced_pod.called)
        self.assertIsNotNone(kc.pod)
        self.assertTrue(mock_follow_logs.called)

    @patch('calrissian.k8s.watch', autospec=True)
    @patch('calrissian.k8s.PodMonitor')
    @patch('calrissian.k8s.KubernetesClient._extract_cpu_memory_requests')
    def test_wait_finishes_when_pod_state_is_terminated(self, mock_cpu_memory,
                                                        mock_podmonitor, mock_watch, mock_get_namespace,
                                                        mock_client):
        mock_pod = create_autospec(V1Pod)
        mock_pod.status.container_statuses[0].state = Mock(running=None, waiting=None, terminated=Mock(exit_code=123))
        mock_cpu_memory.return_value = ('1', '1Mi')
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_pod(Mock())
        completion_result = kc.wait_for_completion()
        self.assertEqual(completion_result.exit_code, 123)
        self.assertTrue(mock_watch.Watch.return_value.stop.called)
        self.assertTrue(mock_client.CoreV1Api.return_value.delete_namespaced_pod.called)
        self.assertIsNone(kc.pod)
        # This is to inspect `with PodMonitor() as monitor`:
        self.assertTrue(mock_podmonitor.return_value.__enter__.return_value.remove.called)

    @patch('calrissian.k8s.watch', autospec=True)
    @patch('calrissian.k8s.KubernetesClient.should_delete_pod')
    @patch('calrissian.k8s.KubernetesClient._extract_cpu_memory_requests')
    def test_wait_checks_should_delete_when_pod_state_is_terminated(self,
                                                                    mock_cpu_memory, mock_should_delete_pod, mock_watch,
                                                                    mock_get_namespace, mock_client):
        mock_pod = create_autospec(V1Pod)
        mock_pod.status.container_statuses[0].state = Mock(running=None, waiting=None, terminated=Mock(exit_code=123))
        mock_cpu_memory.return_value = ('1', '1Mi')
        mock_should_delete_pod.return_value = False
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_pod(Mock())
        completion_result = kc.wait_for_completion()
        self.assertEqual(completion_result.exit_code, 123)
        self.assertEqual(completion_result.memory, '1Mi')
        self.assertEqual(completion_result.cpus, '1')
        self.assertTrue(mock_watch.Watch.return_value.stop.called)
        self.assertFalse(mock_client.CoreV1Api.return_value.delete_namespaced_pod.called)
        self.assertIsNone(kc.pod)

    @patch('calrissian.k8s.watch', autospec=True)
    def test_wait_raises_exception_when_state_is_unexpected(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = create_autospec(V1Pod)
        mock_pod.status.container_statuses[0].state = Mock(running=None, waiting=None, terminated=None)
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_pod(Mock())
        with self.assertRaisesRegex(CalrissianJobException, 'Unexpected pod container status'):
            kc.wait_for_completion()

    def test_raises_on_set_second_pod(self, mock_get_namespace, mock_client):
        kc = KubernetesClient()
        kc._set_pod(Mock())
        with self.assertRaisesRegex(CalrissianJobException, 'This client is already observing pod'):
            kc._set_pod(Mock())

    def test_get_pod_for_name_not_found(self, mock_get_namespace, mock_client):
        mock_client.CoreV1Api.return_value.list_namespaced_pod.return_value = Mock(items=[])
        kc = KubernetesClient()
        with self.assertRaisesRegex(CalrissianJobException,'Unable to find pod with name somepod'):
            kc.get_pod_for_name('somepod')

    def test_get_pod_for_name_one_found(self, mock_get_namespace, mock_client):
        mock_client.CoreV1Api.return_value.list_namespaced_pod.return_value = Mock(items=['pod1'])
        kc = KubernetesClient()
        pod = kc.get_pod_for_name('somepod')
        self.assertEqual(pod, 'pod1')

    def test_get_pod_for_name_multiple_found(self, mock_get_namespace, mock_client):
        mock_client.CoreV1Api.return_value.list_namespaced_pod.return_value = Mock(items=['pod1', 'pod2'])
        kc = KubernetesClient()
        with self.assertRaisesRegex(CalrissianJobException, 'Multiple pods found with name somepod'):
            kc.get_pod_for_name('somepod')

    @patch('calrissian.k8s.os')
    def test_get_current_pod_missing_env_var(self, mock_os, mock_get_namespace, mock_client):
        mock_os.environ = {}
        kc = KubernetesClient()
        with self.assertRaisesRegex(CalrissianJobException, 'Missing required environment variable \$CALRISSIAN_POD_NAME'):
            kc.get_current_pod()

    @patch('calrissian.k8s.os')
    def test_get_current_pod_with_env_var(self, mock_os, mock_get_namespace, mock_client):
        mock_client.CoreV1Api.return_value.list_namespaced_pod.return_value = Mock(
            items=[{'name': 'mypod'}]
        )
        mock_os.environ = {'CALRISSIAN_POD_NAME': 'mypod'}
        kc = KubernetesClient()
        pod = kc.get_current_pod()
        self.assertEqual(pod, {'name': 'mypod'})
        mock_client.CoreV1Api.return_value.list_namespaced_pod.assert_called_with(
            mock_get_namespace.return_value, field_selector='metadata.name=mypod'
        )

    @patch('calrissian.k8s.os')
    def test_should_delete_pod_defaults_yes(self, mock_os, mock_get_namespace, mock_client):
        mock_os.getenv.return_value = ''
        kc = KubernetesClient()
        self.assertTrue(kc.should_delete_pod())
        self.assertEqual(mock_os.getenv.call_args, call('CALRISSIAN_DELETE_PODS', ''))

    @patch('calrissian.k8s.os')
    def test_should_delete_pod_reads_env(self, mock_os, mock_get_namespace, mock_client):
        mock_os.getenv.return_value = 'NO'
        kc = KubernetesClient()
        self.assertFalse(kc.should_delete_pod())
        self.assertEqual(mock_os.getenv.call_args, call('CALRISSIAN_DELETE_PODS', ''))

    def test_delete_pod_name_calls_api(self, mock_get_namespace, mock_client):
        kc = KubernetesClient()
        kc.delete_pod_name('pod-123')
        self.assertEqual('pod-123', mock_client.CoreV1Api.return_value.delete_namespaced_pod.call_args[0][0])

    def test_delete_pod_name_ignores_404(self, mock_get_namespace, mock_client):
        mock_client.CoreV1Api.return_value.delete_namespaced_pod.side_effect = ApiException(status=404)
        kc = KubernetesClient()
        kc.delete_pod_name('pod-123')
        self.assertEqual('pod-123', mock_client.CoreV1Api.return_value.delete_namespaced_pod.call_args[0][0])

    @patch('calrissian.k8s.log')
    def test_follow_logs_streams_to_logging(self, mock_log, mock_get_namespace, mock_client):
        mock_get_namespace.return_value = 'logging-ns'
        mock_read = mock_client.CoreV1Api.return_value.read_namespaced_pod_log
        mock_read.return_value.stream.return_value = [b'line1\n', b'line2\n']
        mock_pod = self.make_mock_pod('logging-pod-123')
        kc = KubernetesClient()
        kc._set_pod(mock_pod)
        mock_log.reset_mock() # log will have other calls before calling follow_logs()
        kc.follow_logs()
        self.assertTrue(mock_read.called)
        self.assertEqual(mock_read.call_args, call('logging-pod-123', 'logging-ns',
                                                   follow=True, _preload_content=False))
        self.assertEqual(mock_log.debug.mock_calls, [
            call('[logging-pod-123] line1'),
            call('[logging-pod-123] line2')
            ])
        self.assertEqual(mock_log.info.mock_calls, [
            call('[logging-pod-123] follow_logs start'),
            call('[logging-pod-123] follow_logs end')
        ])


class KubernetesClientStateTestCase(TestCase):

    def setUp(self):
        self.running_state = Mock(running=Mock(), waiting=None, terminated=None)
        self.waiting_state = Mock(running=None, waiting=Mock(), terminated=None)
        self.terminated_state = Mock(running=None, waiting=None, terminated=Mock())

    def test_is_running(self):
        self.assertTrue(KubernetesClient.state_is_running(self.running_state))
        self.assertFalse(KubernetesClient.state_is_running(self.waiting_state))
        self.assertFalse(KubernetesClient.state_is_running(self.terminated_state))

    def test_is_waiting(self):
        self.assertFalse(KubernetesClient.state_is_waiting(self.running_state))
        self.assertTrue(KubernetesClient.state_is_waiting(self.waiting_state))
        self.assertFalse(KubernetesClient.state_is_waiting(self.terminated_state))

    def test_is_terminated(self):
        self.assertFalse(KubernetesClient.state_is_terminated(self.running_state))
        self.assertFalse(KubernetesClient.state_is_terminated(self.waiting_state))
        self.assertTrue(KubernetesClient.state_is_terminated(self.terminated_state))


class KubernetesClientStatusTestCase(TestCase):

    def setUp(self):
        self.none_statuses = None
        self.empty_list_statuses = []
        self.multiple_statuses = [Mock(), Mock()]
        self.singular_status = [Mock()]

    def test_none_statuses(self):
        self.assertIsNone(KubernetesClient.get_first_or_none(self.none_statuses))
        self.assertIsNone(KubernetesClient.get_first_or_none(self.empty_list_statuses))

    def test_singular_status(self):
        self.assertEqual(len(self.singular_status), 1)
        self.assertIsNotNone(KubernetesClient.get_first_or_none(self.singular_status))

    def test_multiple_statuses_raises(self):
        self.assertEqual(len(self.multiple_statuses), 2)
        with self.assertRaisesRegex(CalrissianJobException, 'Expected 0 or 1 containers, found 2'):
            KubernetesClient.get_first_or_none(self.multiple_statuses)


class PodMonitorTestCase(TestCase):

    def make_mock_pod(self, name):
        mock_metadata = Mock()
        # Cannot mock name attribute without a propertymock
        name_property = PropertyMock(return_value=name)
        type(mock_metadata).name = name_property
        return Mock(metadata=mock_metadata)

    def setUp(self):
        PodMonitor.pod_names = []

    def test_add(self):
        pod = self.make_mock_pod('pod-123')
        self.assertEqual(len(PodMonitor.pod_names), 0)
        with PodMonitor() as monitor:
            monitor.add(pod)
        self.assertEqual(PodMonitor.pod_names, ['pod-123'])

    def test_remove(self):
        pod2 = self.make_mock_pod('pod2')
        PodMonitor.pod_names = ['pod1', 'pod2']
        with PodMonitor() as monitor:
            monitor.remove(pod2)
        self.assertEqual(PodMonitor.pod_names, ['pod1'])

    @patch('calrissian.k8s.KubernetesClient')
    def test_cleanup(self, mock_client):
        mock_delete_pod_name = mock_client.return_value.delete_pod_name
        PodMonitor.pod_names = ['cleanup-pod']
        PodMonitor.cleanup()
        self.assertEqual(mock_delete_pod_name.call_args, call('cleanup-pod'))

    @patch('calrissian.k8s.PodMonitor')
    def test_delete_pods_calls_podmonitor(self, mock_pod_monitor):
        delete_pods()
        self.assertTrue(mock_pod_monitor.cleanup.called)

    @patch('calrissian.k8s.KubernetesClient')
    @patch('calrissian.k8s.log')
    def test_remove_after_cleanup(self, mock_log, mock_client):
        # Depending upon timing cleanup may get called before we receive a remove pod event
        pod = self.make_mock_pod('pod-123')
        with PodMonitor() as monitor:
            monitor.add(pod)
        PodMonitor.cleanup()
        with PodMonitor() as monitor:
            monitor.remove(pod)
        mock_log.info.assert_has_calls([
            call('PodMonitor adding pod-123'),
            call('Starting Cleanup'),
            call('PodMonitor deleting pod pod-123'),
            call('Finishing Cleanup'),
        ])
        mock_log.warning.assert_called_with('PodMonitor pod-123 has already been removed')


class CompletionResultTestCase(TestCase):

    def setUp(self):
        self.pod_name = Mock()
        self.exit_code = Mock()
        self.cpus = Mock()
        self.memory = Mock()
        self.start_time = Mock()
        self.finish_time = Mock()
        self.tool_log = Mock()

    def test_init(self):
        result = CompletionResult(self.exit_code, self.cpus, self.memory, self.start_time, self.finish_time, self.tool_log)
        self.assertEqual(result.exit_code, self.exit_code)
        self.assertEqual(result.cpus, self.cpus)
        self.assertEqual(result.memory, self.memory)
        self.assertEqual(result.start_time, self.start_time)
        self.assertEqual(result.finish_time, self.finish_time)
        self.assertEqual(result.tool_log, self.tool_log)
