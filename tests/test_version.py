from unittest import TestCase
from unittest.mock import patch, call
from importlib.metadata import PackageNotFoundError

from calrissian.version import version, package_version, cwltool_version, calrissian_version


class VersionTestCase(TestCase):

    @patch('calrissian.version.cwltool_version')
    @patch('calrissian.version.calrissian_version')
    def test_assembles_vession_string(self, mock_calrissian_version, mock_cwltool_version):
        mock_cwltool_version.return_value = '3.2.1'
        mock_calrissian_version.return_value = '1.2.3'
        self.assertEqual(version(), 'calrissian 1.2.3 (cwltool 3.2.1)')

    @patch('calrissian.version.installed_version')
    def test_package_version_unknown(self, mock_installed_version):
        mock_installed_version.side_effect = PackageNotFoundError('package-name')
        self.assertEqual(package_version('package-name'), 'unknown')
        mock_installed_version.assert_called_once_with('package-name')

    @patch('calrissian.version.installed_version')
    def test_package_version_known(self, mock_installed_version):
        mock_installed_version.return_value = '1.2.3'
        self.assertEqual(package_version('package-name'), '1.2.3')
        mock_installed_version.assert_called_once_with('package-name')

    @patch('calrissian.version.package_version')
    def test_cwltool_version(self, mock_package_version):
        version = cwltool_version()
        self.assertEqual(mock_package_version.call_args, call('cwltool'))
        self.assertEqual(version, mock_package_version.return_value)

    @patch('calrissian.version.package_version')
    def test_calrissian_version(self, mock_package_version):
        version = calrissian_version()
        self.assertEqual(mock_package_version.call_args, call('calrissian'))
        self.assertEqual(version, mock_package_version.return_value)
