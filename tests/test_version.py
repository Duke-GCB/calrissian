from unittest import TestCase
from calrissian.version import version


class VersionTestCase(TestCase):

    def test_version(self):
        self.assertEqual(version(), 'Calrissian-dev')

