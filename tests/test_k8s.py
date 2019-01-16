from unittest import TestCase
from unittest.mock import Mock, patch, call
from calrissian.k8s import load_config_get_namespace, KubernetesClient, CalrissianJobException


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
        mock_config.ConfigException = Exception
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
        self.assertEqual(kc.batch_api_instance, mock_client.BatchV1Api.return_value)
        self.assertEqual(kc.core_api_instance, mock_client.CoreV1Api.return_value)
        self.assertIsNone(kc.job)
        self.assertIsNone(kc.process_exit_code)

    def test_submit_job(self, mock_get_namespace, mock_client):
        mock_get_namespace.return_value = 'namespace'
        mock_create_namespaced_job = Mock()
        mock_create_namespaced_job.return_value = Mock(metadata=Mock(uid='123'))
        mock_client.BatchV1Api.return_value.create_namespaced_job = mock_create_namespaced_job
        kc = KubernetesClient()
        mock_body = Mock()
        kc.submit_job(mock_body)
        self.assertEqual(kc.job.metadata.uid, '123')
        self.assertEqual(mock_create_namespaced_job.call_args, call('namespace', mock_body))

    def setup_mock_watch(self, mock_watch, event_objects=[]):
        mock_stream = Mock()
        mock_stop = Mock()
        stream_events = []
        for event_object in event_objects:
            stream_events.append({'object': event_object})
        mock_stream.return_value = stream_events
        mock_watch.Watch.return_value.stream = mock_stream
        mock_watch.Watch.return_value.stop = mock_stop

    @patch('calrissian.k8s.watch')
    def test_wait_calls_watch_pod_with_job_uid_label_selector(self, mock_watch, mock_get_namespace, mock_client):
        self.setup_mock_watch(mock_watch)
        kc = KubernetesClient()
        kc._set_job(Mock(metadata=Mock(uid='456')))
        kc.wait_for_completion()
        mock_stream = mock_watch.Watch.return_value.stream
        self.assertEqual(mock_stream.call_args, call(kc.core_api_instance.list_namespaced_pod, kc.namespace, label_selector='controller-uid=456'))

    @patch('calrissian.k8s.watch')
    def test_wait_skips_pod_when_status_is_none(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = Mock(status=Mock(container_statuses=None))
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_job(Mock(metadata=Mock(uid='456')))
        kc.wait_for_completion()
        self.assertFalse(mock_watch.Watch.return_value.stop.called)
        self.assertFalse(mock_client.BatchV1Api.return_value.delete_namespaced_job.called)
        self.assertIsNotNone(kc.job)

    @patch('calrissian.k8s.watch')
    def test_wait_skips_pod_when_state_is_running(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = Mock(status=Mock(container_statuses=[Mock(state=Mock(running=Mock(), terminated=None, waiting=None))]))
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_job(Mock(metadata=Mock(uid='456')))
        kc.wait_for_completion()
        self.assertFalse(mock_watch.Watch.return_value.stop.called)
        self.assertFalse(mock_client.BatchV1Api.return_value.delete_namespaced_job.called)
        self.assertIsNotNone(kc.job)

    @patch('calrissian.k8s.watch')
    def test_wait_raises_exception_when_state_is_unexpected(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = Mock(status=Mock(container_statuses=[Mock(state=Mock(running=None, terminated=Mock(exit_code=123), waiting=None))]))
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_job(Mock(metadata=Mock(uid='456')))
        exit_code = kc.wait_for_completion()
        self.assertEqual(exit_code, 123)

    @patch('calrissian.k8s.watch')
    def test_wait_finishes_when_pod_state_is_terminated(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = Mock(status=Mock(container_statuses=[Mock(state=Mock(running=None, terminated=None, waiting=None))]))
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_job(Mock(metadata=Mock(uid='456')))
        with self.assertRaises(CalrissianJobException) as context:
            kc.wait_for_completion()
        self.assertIn('Unexpected pod container status', str(context.exception))

    @patch('calrissian.k8s.watch')
    def test_wait_returns_exit_code(self, mock_watch, mock_get_namespace, mock_client):
        mock_pod = Mock(status=Mock(container_statuses=[Mock(state=Mock(running=None, terminated=Mock(exit_code=123), waiting=None))]))
        self.setup_mock_watch(mock_watch, [mock_pod])
        kc = KubernetesClient()
        kc._set_job(Mock(metadata=Mock(uid='456')))
        exit_code = kc.wait_for_completion()
        self.assertEqual(exit_code, 123)

    def test_raises_on_set_second_job(self, mock_get_namespace, mock_client):
        kc = KubernetesClient()
        kc._set_job(Mock(metadata=Mock(uid='123')))
        with self.assertRaises(CalrissianJobException) as context:
            kc._set_job(Mock(metadata=Mock(uid='123')))
        self.assertIn('his client is already observing job', str(context.exception))


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
        self.assertIsNone(KubernetesClient.get_first_status_or_none(self.none_statuses))
        self.assertIsNone(KubernetesClient.get_first_status_or_none(self.empty_list_statuses))

    def test_singular_status(self):
        self.assertEqual(len(self.singular_status), 1)
        self.assertIsNotNone(KubernetesClient.get_first_status_or_none(self.singular_status))

    def test_multiple_statuses_raises(self):
        self.assertEqual(len(self.multiple_statuses), 2)
        with self.assertRaises(CalrissianJobException) as context:
            KubernetesClient.get_first_status_or_none(self.multiple_statuses)
        self.assertIn('Expected 0 or 1 container statuses in job, found 2', str(context.exception))
