#!/usr/bin/env python

from setuptools import find_packages, setup

import NDATools

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
        name='nda_tools',
        description="NIMH Data Archive Python Client",
        install_requires=['boto3', 'botocore', 'tqdm', 'requests', 'mock', 'packaging','pyyaml', 'keyring'],
        extras_require={'test': ['pytest', 'pytest-datadir']},
        version= NDATools.__version__,
        long_description=long_description,
        long_description_content_type="text/markdown",
        author='NDA',
        author_email='NDAHelp@mail.nih.gov',
        url="https://github.com/NDAR/nda-tools/tree/master/NDATools",
        license='MIT',
        packages=find_packages(),
        include_package_data=True,
        data_files=[('config', ['NDATools/clientscripts/config/settings.cfg', 'NDATools/clientscripts/config/logging.yml'])],
        entry_points={
            'console_scripts': [
                'vtcmd = NDATools.clientscripts.vtcmd:main',
                'downloadcmd = NDATools.clientscripts.downloadcmd:main',
                'unit_tests = tests.run_unit_tests:main',
                'integration_tests = tests.run_integration_tests:main']
        }
    )
