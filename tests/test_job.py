from unittest import TestCase
from unittest.mock import Mock
from calrissian.job import k8s_safe_name, KubernetesVolumeBuilder, VolumeBuilderException, KubernetesJobBuilder


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
