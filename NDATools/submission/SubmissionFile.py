import os

import botocore

from NDATools.utils import Utils


class SubmissionFile():
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.abs_path = None
        self.size = None
        self.has_read_access = False

    def find_and_set_local_file_info(self, directory_list):

        def check_read_permissions(file):
            try:
                open(file)
                return True
            except (OSError, IOError) as err:
                if err.errno == 13:
                    print('Permission Denied: {}'.format(file))
            return False

        for d in directory_list:
            self.abs_path = None
            if os.path.isfile(self.csv_path):
                self.abs_path = os.path.abspath(self.csv_path)
            elif os.path.isfile(os.path.join(d, self.csv_path)):
                self.abs_path = os.path.abspath(os.path.join(d, self.csv_path))

        self.has_read_access = check_read_permissions(self.abs_path)
        self.size = os.path.getsize(self.abs_path)

    def find_and_set_s3_file_info(self, s3_client, source_bucket, source_prefix=''):
        if self.csv_path.lower().starts_with('s3://'):
            self.abs_path = self.csv_path
        else:
            self.abs_path = 's3://{}/{}{}'.format(source_bucket, source_prefix, self.csv_path)

        bucket , key = Utils.deconstruct_s3_url(self.abs_path)
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
            self.size = int(response['ContentLength'])
            self.has_read_access = True
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                self.abs_path = None # reset abs path since we dont know where it is
            if error_code == 403:
                self.has_read_access = False