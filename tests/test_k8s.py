from unittest import TestCase
from unittest.mock import Mock, patch, call, PropertyMock
from calrissian.k8s import load_config_get_namespace, KubernetesClient, CalrissianJobException, PodMonitor, delete_pods, Reporter

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
        mock_config.config_exception.ConfigException = Exception
        mock_config.load_incluster_config.side_effect = Exception
        namespace = load_config_get_namespace()
        self.assertEqual(namespace, 'default')
        self.assertFalse(mock_read_file.called)
        self.assertTrue(mock_config.load_incluster_config.called)
        self.assertTrue(mock_config.load_kube_config.called)


@patch('calrissian.k8s.client')
@patch('calrissian.k8s.load_config_get_namespace')
class KubernetesClientTestCase(TestCase):

    def test_init(self, mock_get_namespace, mock_client):
        kc = KubernetesClient()
        self.assertEqual(kc.namespace, mock_get_namespace.return_value)
        self.assertEqual(kc.core_api_instance, mock_client.CoreV1Api.return_value)
        self.assertIsNone(kc.pod)
        self.assertIsNone(kc.process_exit_code)

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
        name_property = PropertyMock(return_value='test123')
        type(mock_metadata).name = name_property
        return Mock(metadata=mock_metadata)

    @patch('calrissian.k8s.watch')
    def test_wait_calls_watch_pod_with_pod_name_field_selector(self, mock_watch, mock_get_namespace, mock_client):
        self.setup_mock_watch(mock_watch)
        mock_pod = self.make_mock_pod('test123')
        kc = KubernetesClient()
        kc._set_pod(mock_pod)
        kc.wait_for_completion()
        mock_stream = mock_watch.Watch.return_value.stream
        self.assertEqual(mock_stream.call_args, call(kc.core_api_instance.list_namespaced_pod, kc.namespace, field_selector='metadata.name=test123'))

    @patch('calrissian.k8s.watch')
    def test_wait_skips_pod_when_status_is_none(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = Mock(status=Mock(container_statuses=None))
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_pod(Mock())
        kc.wait_for_completion()
        self.assertFalse(mock_watch.Watch.return_value.stop.called)
        self.assertFalse(mock_client.CoreV1Api.return_value.delete_namespaced_pod.called)
        self.assertIsNotNone(kc.pod)

    @patch('calrissian.k8s.watch')
    def test_wait_skips_pod_when_state_is_running(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = Mock(status=Mock(container_statuses=[Mock(state=Mock(running=Mock(), terminated=None, waiting=None))]))
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_pod(Mock())
        kc.wait_for_completion()
        self.assertFalse(mock_watch.Watch.return_value.stop.called)
        self.assertFalse(mock_client.CoreV1Api.return_value.delete_namespaced_pod.called)
        self.assertIsNotNone(kc.pod)

    @patch('calrissian.k8s.watch')
    @patch('calrissian.k8s.PodMonitor')
    @patch('calrissian.k8s.Reporter')
    @patch('calrissian.k8s.KubernetesClient._extract_cpu_memory_requests')
    def test_wait_finishes_when_pod_state_is_terminated(self, mock_cpu_memory, mock_reporter, mock_podmonitor, mock_watch, mock_get_namespace, mock_client):
        mock_pod = Mock(status=Mock(container_statuses=[Mock(state=Mock(running=None, terminated=Mock(exit_code=123), waiting=None))]))
        mock_pod.spec = Mock(containers=[])
        mock_cpu_memory.return_value = ('1', '1Mi')
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_pod(Mock())
        exit_code = kc.wait_for_completion()
        self.assertEqual(exit_code, 123)
        self.assertTrue(mock_watch.Watch.return_value.stop.called)
        self.assertTrue(mock_client.CoreV1Api.return_value.delete_namespaced_pod.called)
        self.assertIsNone(kc.pod)
        # This is to inspect `with PodMonitor() as monitor`:
        self.assertTrue(mock_podmonitor.return_value.__enter__.return_value.remove.called)
        self.assertTrue(mock_reporter.return_value.__enter__.return_value.report.called)

    @patch('calrissian.k8s.watch')
    @patch('calrissian.k8s.KubernetesClient.should_delete_pod')
    @patch('calrissian.k8s.KubernetesClient._extract_cpu_memory_requests')
    def test_wait_checks_should_delete_when_pod_state_is_terminated(self, mock_cpu_memory, mock_should_delete_pod, mock_watch, mock_get_namespace, mock_client):
        mock_pod = Mock(status=Mock(container_statuses=[Mock(state=Mock(running=None, terminated=Mock(exit_code=123), waiting=None))]))
        mock_pod.spec = Mock(containers=[])
        mock_cpu_memory.return_value = ('1', '1Mi')
        mock_should_delete_pod.return_value = False
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_pod(Mock())
        exit_code = kc.wait_for_completion()
        self.assertEqual(exit_code, 123)
        self.assertTrue(mock_watch.Watch.return_value.stop.called)
        self.assertFalse(mock_client.CoreV1Api.return_value.delete_namespaced_pod.called)
        self.assertIsNone(kc.pod)

    @patch('calrissian.k8s.watch')
    def test_wait_raises_exception_when_state_is_unexpected(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = Mock(status=Mock(container_statuses=[Mock(state=Mock(running=None, terminated=None, waiting=None))]))
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_pod(Mock())
        with self.assertRaises(CalrissianJobException) as context:
            kc.wait_for_completion()
        self.assertIn('Unexpected pod container status', str(context.exception))

    def test_raises_on_set_second_pod(self, mock_get_namespace, mock_client):
        kc = KubernetesClient()
        kc._set_pod(Mock())
        with self.assertRaises(CalrissianJobException) as context:
            kc._set_pod(Mock())
        self.assertIn('This client is already observing pod', str(context.exception))

    def test_get_pod_for_name_not_found(self, mock_get_namespace, mock_client):
        mock_client.CoreV1Api.return_value.list_namespaced_pod.return_value = Mock(items=[])
        kc = KubernetesClient()
        with self.assertRaises(CalrissianJobException) as raised_exception:
            kc.get_pod_for_name('somepod')
        self.assertEqual(str(raised_exception.exception), 'Unable to find pod with name somepod')

    def test_get_pod_for_name_one_found(self, mock_get_namespace, mock_client):
        mock_client.CoreV1Api.return_value.list_namespaced_pod.return_value = Mock(items=['pod1'])
        kc = KubernetesClient()
        pod = kc.get_pod_for_name('somepod')
        self.assertEqual(pod, 'pod1')

    def test_get_pod_for_name_multiple_found(self, mock_get_namespace, mock_client):
        mock_client.CoreV1Api.return_value.list_namespaced_pod.return_value = Mock(items=['pod1', 'pod2'])
        kc = KubernetesClient()
        with self.assertRaises(CalrissianJobException) as raised_exception:
            kc.get_pod_for_name('somepod')
        self.assertEqual(str(raised_exception.exception), 'Multiple pods found with name somepod')

    @patch('calrissian.k8s.os')
    def test_get_current_pod_missing_env_var(self, mock_os, mock_get_namespace, mock_client):
        mock_os.environ = {}
        kc = KubernetesClient()
        with self.assertRaises(CalrissianJobException) as raised_exception:
            kc.get_current_pod()
        self.assertEqual(str(raised_exception.exception),
                         'Missing required environment variable $CALRISSIAN_POD_NAME')

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

    def test_delete_pod_name_raises(self, mock_get_namespace, mock_client):
        mock_client.rest.ApiException = Exception
        mock_client.CoreV1Api.return_value.delete_namespaced_pod.side_effect = mock_client.rest.ApiException
        kc = KubernetesClient()
        with self.assertRaises(CalrissianJobException) as context:
            kc.delete_pod_name('pod-123')
        self.assertIn('Error deleting pod named pod-123', str(context.exception))


class KubernetesClientStateTestCase(TestCase):

    def setUp(self):
        self.running_state = Mock(running=Mock(), waiting=None, terminated=None)
        self.waiting_state = Mock(running=None, waiting=Mock(), terminated=None)
        self.terminated_state = Mock(running=None, waiting=None, terminated=Mock())

    def test_is_running(self):
        self.assertTrue(KubernetesClient.state_is_running(self.running_state))
        self.assertTrue(KubernetesClient.state_is_running(self.waiting_state))
        self.assertFalse(KubernetesClient.state_is_running(self.terminated_state))

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
        with self.assertRaises(CalrissianJobException) as context:
            KubernetesClient.get_first_or_none(self.multiple_statuses)
        self.assertIn('Expected 0 or 1 containers, found 2', str(context.exception))


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


class ReporterTestCase(TestCase):

    def setUp(self):
        Reporter.clear()

    @patch('calrissian.k8s.Reporter.timeline_report.add_report')
    def test_report(self, mock_add_report):
        # def report(self, cpus, ram_megabytes, start_time, finish_time):
        start_time = Mock()
        finish_time = Mock()
        with Reporter() as reporter:
            reporter.report('1', '1024Mi', start_time, finish_time)
        added_report = mock_add_report.call_args[0][0]
        self.assertEqual(added_report.start_time, start_time)
        self.assertEqual(added_report.finish_time, finish_time)
        self.assertEqual(added_report.cpus, 1)
        self.assertEqual(added_report.ram_megabytes, 1024)

    def test_get_report(self):
        mock_timeline_report = Mock()
        Reporter.timeline_report = mock_timeline_report
        self.assertEqual(Reporter.get_report(), mock_timeline_report)
