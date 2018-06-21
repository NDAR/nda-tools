from NDATools.Validation import Validation
from NDATools.BuildPackage import SubmissionPackage
from NDATools.Submission import Submission
from NDATools.nda_aws_token_generator import *
if sys.version_info[0] < 3:
    input = raw_input
    import thread

import getpass

username = input('Enter your NIMH Data Archives username:')
password = getpass.getpass('Enter your NIMH Data Archives password:')

generator = NDATokenGenerator()

token = generator.generate_token(username, password)

print('aws_access_key_id=%s\n'
      'aws_secret_access_key=%s\n'
      'security_token=%s\n'
      'expiration=%s\n'
      %(token.access_key,
        token.secret_key,
        token.session,
        token.expiration)
      )


files = ['/Users/ahmadus/Documents/Client/testdata/no_associated_files/abc_community02_lowercase_guid.csv',
         '/Users/ahmadus/Documents/Client/testdata/no_associated_files/abc_community02.csv',
         '/Users/ahmadus/Documents/Client/testdata/with_associated_files/image03_condition.csv',
         '/Users/ahmadus/Documents/Client/testdata/with_associated_files/genomics_sample03_good_relative_path.csv',
         '/Users/ahmadus/Documents/Client/testdata/with_associated_files/sample03-experiment.csv']


validate = Validation(files)
validate.validate()
validate.output() #maybe change to "save_errors()"
validate.get_warnings() #maybe change to "save_warnings()" #maybe merge with save_errors()?

uuid = validate.uuid
associated_files = validate.associated_files
new_ids = validate.verify_uuid() # creates a new list of only valid (no errors) uuid to package for submission

#uuid must be a LIST, pass in parameters OR a config object instead
package = SubmissionPackage(new_ids, associated_files, username=username, password=password, t='test', d='test', c=1860)

package.set_upload_destination()

# directories MUST be a list
dir_list = ['/Users/ahmadus/Documents/Client/testdata/with_associated_files']
if associated_files:
    package.file_search(dir_list)


package.build_package()
package.download_package()
# print('\nPackage finished building.\n')

packageID = package.package_id
full_file_path = package.full_file_path

submission = Submission(packageID, full_file_path, username=username, password=password, resume=False)
submission.submit()
if associated_files:
    print('Preparing to upload associated files.')
    submission.submission_upload()
#submission = Submission('15206', full_file_path=None, username=username, password=password, resume=True)
#submission.check_status()
