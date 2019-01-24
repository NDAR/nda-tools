import pytest
from NDATools.Configuration import *
from NDATools.Validation import Validation
from NDATools.Submission import Submission

"""
def test_submission(): # cannot reuse submission_id so need to do integration tests maybe...?
	config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
	full_file_path = []
	submission = Submission(id='ce80eeab-d9b5-46f0-bde5-d09d3f9f0cbd', full_file_path=full_file_path, allow_exit=True, config=config)
	submission.submit()
	#4e722093-9a8f-4883-9395-58a010fdce00 uploading
	#a640173c-0350-4df6-b0c6-2f1804c88f57 complete

	assert submission.status == "Complete"
"""


def test_status():
	config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
	full_file_path = []
	submission = Submission(id='ce80eeab-d9b5-46f0-bde5-d09d3f9f0cbd', full_file_path=full_file_path, allow_exit=True,
	                        config=config)
	submission.submission_id = '18548'
	submission.check_status()

	assert submission.status == "Submitted_Prototype" #"Upload Completed"


