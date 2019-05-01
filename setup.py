#!/usr/bin/env python

from setuptools import find_packages, setup


with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
        name='nda_tools',
        description="NIMH Data Archive Python Client",
        long_description=long_description,
        long_description_content_type="text/markdown",
        install_requires=['boto3>=1.4.7', 'botocore>=1.7.48', 'tqdm','requests'],
        version='0.1.18',
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
                'downloadcmd = NDATools.clientscripts.downloadcmd:main']
        }
    )
