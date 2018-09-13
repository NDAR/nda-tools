from setuptools import find_packages, setup


setup(
        name='nda_tools-test',
        description="NIMH Data Archive Python Client",
        install_requires=['boto3', 'tqdm','requests'],
        version='0.1.3',
        author='NDA',
        author_email='NDAHelp@mail.nih.gov',
        url="https://data-archive.nimh.nih.gov",
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