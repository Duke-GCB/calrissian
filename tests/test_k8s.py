from unittest import TestCase
from unittest.mock import Mock, patch
from calrissian.k8s import load_config_get_namespace, KubernetesClient


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

