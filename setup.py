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