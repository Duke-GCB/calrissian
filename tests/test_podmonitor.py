from unittest import TestCase
from unittest.mock import Mock, patch, call, PropertyMock
from calrissian.podmonitor import PodMonitor, delete_pods
from calrissian.k8s import KubernetesClient

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
        PodMonitor.add(pod)
        self.assertEqual(PodMonitor.pod_names, ['pod-123'])

    def test_remove(self):
        pod2 = self.make_mock_pod('pod2')
        PodMonitor.pod_names = ['pod1', 'pod2']
        PodMonitor.remove(pod2)
        self.assertEqual(PodMonitor.pod_names, ['pod1'])

    def test_cleanup(self):
        mock_client_cls = Mock()
        mock_delete_pod_name = mock_client_cls.return_value.delete_pod_name
        PodMonitor.pod_names = ['cleanup-pod']
        PodMonitor.cleanup(mock_client_cls)
        self.assertEqual(mock_delete_pod_name.call_args, call('cleanup-pod'))

    @patch('calrissian.podmonitor.PodMonitor')
    def test_delete_pods_calls_podmonitor_with_kc(self, mock_pod_monitor):
        delete_pods()
        self.assertEqual(mock_pod_monitor.cleanup.call_args, call(KubernetesClient))
