from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='calrissian',
    version='0.1.0',
    author='Dan Leehr',
    author_email='dan.leehr@duke.edu',
    description='CWL runner for Kubernetes',
    install_requires=[
        'cwltool==1.0.20181217162649',
        'kubernetes==8.0.1',
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
)
