from unittest import TestCase, mock
from calrissian.job import k8s_safe_name, KubernetesVolumeBuilder, VolumeBuilderException


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


