from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as installed_version


def package_version(package_name):
    try:
        return installed_version(package_name)
    except PackageNotFoundError:
        return 'unknown'


def cwltool_version():
    return package_version('cwltool')


def calrissian_version():
    return package_version('calrissian')


def version():
    return 'calrissian {} (cwltool {})'.format(calrissian_version(), cwltool_version())
