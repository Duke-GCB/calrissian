import os

from unittest import TestCase
from unittest.mock import patch, call, Mock, mock_open
import logging

from cwltool.utils import CWLObjectType

from calrissian.dask import (
    KubernetesDaskPodBuilder
)

from calrissian.dask import (
    dask_req_validate,
)


class ValidateExtensionTestCase(TestCase):

    def setUp(self):
        self.daskGatewayRequirement: CWLObjectType = {
            "workerCores": 2,
            "workerCoresLimit": 2,
            "workerMemory": "4G",
            "clustermaxCore": 8,
            "clusterMaxMemory": "16G",
            "class": "https://calrissian-cwl.github.io/schema#DaskGatewayRequirement" # From cwl
        }
    
    def tests_validate_extension(self):
        self.assertTrue(dask_req_validate(self.daskGatewayRequirement))


class KubernetesDaskPodBuilderTestCase(TestCase):

    def setUp(self):
        self.name = 'PodName'
        self.container_image = 'dockerimage:1.0'
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
        self.dask_gateway_url = "http://dask-gateway-url:80"
        self.dask_gateway_controller = False
        self.environment = {
            'HOME': '/homedir',
            'PYTHONPATH': '/app',
        }
        self.pod_builder = KubernetesDaskPodBuilder(self.name, self.container_image, self.environment, self.volume_mounts,
                                                self.volumes, self.command_line, self.stdout, self.stderr, self.stdin,
                                                self.resources, self.labels, self.nodeselectors, self.security_context, self.pod_serviceaccount,
                                                self.dask_gateway_url, self.dask_gateway_controller)
        self.pod_builder.dask_requirement = {
            "workerCores": 2,
            "workerCoresLimit": 2,
            "workerMemory": "4G",
            "clustermaxCore": 8,
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
