import pkg_resources  # part of setuptools


def package_version(package_name):
    try:
        pkg = pkg_resources.require(package_name)
        return pkg[0].version
    except pkg_resources.DistributionNotFound:
        return 'unknown'


def cwltool_version():
    return package_version('cwltool')


def calrissian_version():
    return package_version('calrissian')


def version():
    return 'calrissian {} (cwltool {})'.format(calrissian_version(), cwltool_version())
