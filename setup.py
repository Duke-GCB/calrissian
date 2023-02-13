import os
import sys
from setuptools import setup
# version checking derived from https://github.com/levlaz/circleci.py/blob/master/setup.py
from setuptools.command.install import install

VERSION = '0.12.0'
TAG_ENV_VAR = 'CIRCLE_TAG'

with open("README.md", "r") as fh:
    long_description = fh.read()


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""
    description = 'verify that the git tag matches our version'

    def run(self):
        tag = os.getenv(TAG_ENV_VAR)

        if tag != VERSION:
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, VERSION
            )
            sys.exit(info)


setup(
    name='calrissian',
    version=VERSION,
    author='Dan Leehr',
    author_email='dan.leehr@duke.edu',
    description='CWL runner for Kubernetes',
    install_requires=[
        'urllib3>=1.24.2,<1.27',
        'kubernetes==10.0.1',
        'cwltool==3.1.20230201224320',
        'tenacity==5.1.1',
        'importlib-metadata<5,>=0.23'
    ],
    test_suite='nose2.collector.collector',
    tests_require=['nose2'],
    entry_points={
        'console_scripts': [
            'calrissian = calrissian.main:main'
        ]
    },
    license='MIT',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/Duke-GCB/calrissian',
    packages=['calrissian',],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: System :: Distributed Computing',
    ],
    cmdclass={
        'verify': VerifyVersionCommand,
    },
)
