from NDATools.DataManager import DataManager
import boto3


class S3Authentication:

    def __init__(self, config):
        self.config = config
        if config.aws_secret_key == "" and config.aws_access_key == "":
            self.aws_access_key, self.aws_secret_key, self.aws_session_token = DataManager(config).credentials
        else:
            self.aws_access_key = config.aws_access_key
            self.aws_secret_key = config.aws_secret_key
            if config.aws_session_token != "":
                self.aws_session_token = config.aws_session_token
        self.credentials = self.get_credentials

    @property
    def get_credentials(self):
        return {'aws_access_key_id': self.aws_access_key,
                'aws_secret_access_key': self.aws_secret_key,
                'aws_session_token': self.aws_session_token}

    def get_s3_client(self):
        return boto3.session.Session(**self.credentials).client('s3')
