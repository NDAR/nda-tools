<<<<<<< HEAD
from setuptools import setup


setup(name='nda_tools',
      version='0.1.3',
      description="NIMH Data Archive Python Client",
      install_requires=['boto3','tqdm','requests'],
      author='NDA',
      author_email='NDAHelp@mail.nih.gov',
      url="https://data-archive.nimh.nih.gov",
      license='MIT',
      include_package_data=True,
      py_modules=['nda_tools']
      )
=======
from setuptools import find_packages, setup


setup(
        name='nda_tools',
        description="NIMH Data Archive Python Client",
        install_requires=['boto3==1.4.7', 'botocore==1.7.48', 'tqdm','requests'],
        version='0.4.9',
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
>>>>>>> dcd40d75f53dd08980e2c3e4a0de175d0c653674
