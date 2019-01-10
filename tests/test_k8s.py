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

    def test_submit_job(self, mock_get_namespace, mock_client):
        mock_get_namespace.return_value = 'namespace'
        mock_create_namespaced_job = Mock()
        mock_create_namespaced_job.return_value = Mock(metadata=Mock(uid='123'))
        mock_client.BatchV1Api.return_value.create_namespaced_job = mock_create_namespaced_job
        kc = KubernetesClient()
        mock_body = Mock()
        kc.submit_job(mock_body)
        self.assertEqual(kc.job_uid, '123')
        self.assertEqual(mock_create_namespaced_job.call_args, call('namespace', mock_body))

    def setup_mock_watch(self, mock_watch, event):
        mock_stream = Mock()
        mock_stop = Mock()
        mock_stream.return_value = [{'object': event}]
        mock_watch.Watch.return_value.stream = mock_stream
        mock_watch.Watch.return_value.stop = mock_stop

    @patch('calrissian.k8s.watch')
    def test_wait_successful_job(self, mock_watch, mock_get_namespace, mock_client):
        kc = KubernetesClient()
        kc.watch_job(Mock(metadata=Mock(uid='456')))
        success_job_event = Mock(status=Mock(succeeded=True, failed=False), metadata=Mock(uid='456'))
        self.setup_mock_watch(mock_watch, success_job_event)
        kc.wait()
        self.assertTrue(mock_watch.Watch.return_value.stream.called)
        self.assertTrue(mock_watch.Watch.return_value.stop.called)
        self.assertIsNone(kc.job_uid)
        # job should be deleted
        self.assertTrue(mock_client.BatchV1Api.return_value.delete_namespaced_job.called)

    @patch('calrissian.k8s.watch')
    def test_wait_failed_job_raises(self, mock_watch, mock_get_namespace, mock_client):
        kc = KubernetesClient()
        kc.watch_job(Mock(metadata=Mock(uid='456')))
        failed_job_event = Mock(status=Mock(succeeded=False, failed=True), metadata=Mock(uid='456'))
        self.setup_mock_watch(mock_watch, failed_job_event)
        with self.assertRaises(CalrissianJobException):
            kc.wait()
        self.assertIsNone(kc.job_uid)
        self.assertTrue(mock_watch.Watch.return_value.stream.called)
        self.assertFalse(mock_watch.Watch.return_value.stop.called)
        # job should not be deleted
        self.assertFalse(mock_client.BatchV1Api.return_value.delete_namespaced_job.called)

    @patch('calrissian.k8s.watch')
    def test_ignores_other_job_ids(self, mock_watch, mock_get_namespace, mock_client):
        kc = KubernetesClient()
        kc.watch_job(Mock(metadata=Mock(uid='456')))
        other_job_event = Mock(status=Mock(succeeded=True, failed=False), metadata=Mock(uid='789'))
        self.setup_mock_watch(mock_watch, other_job_event)
        kc.wait()
        self.assertIsNotNone(kc.job_uid)
        # None of kc's critical state should have changed
        self.assertTrue(mock_watch.Watch.return_value.stream.called)
        self.assertFalse(mock_watch.Watch.return_value.stop.called)
        # job should not be deleted
        self.assertFalse(mock_client.BatchV1Api.return_value.delete_namespaced_job.called)

    def test_raises_on_watch_second_job(self, mock_get_namespace, mock_client):
        kc = KubernetesClient()
        kc.watch_job(Mock(metadata=Mock(uid='123')))
        with self.assertRaises(CalrissianJobException):
            kc.watch_job(Mock(metadata=Mock(uid='123')))
