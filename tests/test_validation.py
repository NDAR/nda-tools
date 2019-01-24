import pytest
from NDATools.Configuration import *
from NDATools.Validation import Validation


def test_validation():
	config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
	file_list = ['/Users/ahmadus/Documents/Client/testdata/no_associated_files/abc_community02.csv']

	validation = Validation(file_list, config=config, allow_exit=True)
	validation.validate()
	for (response, file) in validation.responses:
		status = response['status']

	assert status =='CompleteWithWarnings'


def test_validation_manifest():
	config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
	file_list = ['/Users/ahmadus/Documents/Client/imagingcollection01_exampledata1/ManifestTestData/NDADataStructures/sample_imagingcollection01_one_record.csv']
	config.manifest_path = ['/Users/ahmadus/Documents/Client/imagingcollection01_exampledata1/ManifestTestData/HCPManifests']

	validation = Validation(file_list, config=config, allow_exit=True)
	validation.validate()
	for (response, file) in validation.responses:
		status = response['status']

	assert status =='PendingManifestFiles' # but it should be COMPLETE. WHy is it not updating??


def test_validation_output():
	config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
	file_list = ['/Users/ahmadus/Documents/Client/testdata/no_associated_files/abc_community02.csv']

	validation = Validation(file_list, config=config, allow_exit=True)
	validation.validate()
	validation.output()

	assert os.path.isfile(validation.log_file)


def test_warning_output():
	config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
	file_list = ['/Users/ahmadus/Documents/Client/testdata/no_associated_files/abc_community02.csv']

	validation = Validation(file_list, config=config, allow_exit=True)
	validation.validate()
	validation.get_warnings()
	new_path = ''.join([validation.validation_result_dir, '/validation_warnings_', validation.date, '.csv'])

	assert os.path.isfile(new_path)
