from unittest import TestCase
from unittest.mock import Mock, patch

from calrissian.version import version, package_version


class VersionTestCase(TestCase):

    @patch('calrissian.version.cwltool_version')
    @patch('calrissian.version.calrissian_version')
    def test_assembles_vession_string(self, mock_calrissian_version, mock_cwltool_version):
        mock_cwltool_version.return_value = '3.2.1'
        mock_calrissian_version.return_value = '1.2.3'
        self.assertEqual(version(), 'calrissian 1.2.3 (cwltool 3.2.1)')

    @patch('calrissian.version.pkg_resources')
    def test_package_version_unknown(self, mock_pkg_resources):
        mock_pkg_resources.DistributionNotFound = Exception
        mock_pkg_resources.require.side_effect = mock_pkg_resources.DistributionNotFound()
        self.assertEqual(package_version('package-name'), 'unknown')
        self.assertTrue(mock_pkg_resources.require.called)

    @patch('calrissian.version.pkg_resources')
    def test_package_version_known(self, mock_pkg_resources):
        mock_pkg_resources.require.return_value = [Mock(version='1.2.3'), ]
        self.assertEqual(package_version('package-name'), '1.2.3')
        self.assertTrue(mock_pkg_resources.require.called)
