#!/usr/bin/env python

import NDATools
import sys

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = ['boto3', 'botocore', 'tqdm', 'requests', 'mock', 'pytest']

# Only include the backport libraries if the current version of python requires it
if sys.version_info.major < 3:
    install_requires.append('funcsigs')
    install_requires.append('python-dateutil==2.2')

if sys.version_info < (3, 4):
    install_requires.append('enum34')

setup(
        name='nda_tools',
        description="NIMH Data Archive Python Client",
        install_requires=install_requires,
        version=NDATools.__version__,
        long_description=long_description,
        long_description_content_type="text/markdown",
        author='NDA',
        author_email='NDAHelp@mail.nih.gov',
        url="https://github.com/NDAR/nda-tools/tree/master/NDATools",
        license='MIT',
        packages=find_packages(),
        include_package_data=True,
        data_files=[('config', ['NDATools/clientscripts/config/settings.cfg'])],
        entry_points={
            'console_scripts': [
                'vtcmd = NDATools.clientscripts.vtcmd:main',
                'downloadcmd = NDATools.clientscripts.downloadcmd:main',
                'mindar = NDATools.clientscripts.mindar:main',
                'unit_tests = tests.run_unit_tests:main',
                'integration_tests = tests.run_integration_tests:main']
        }
    )
