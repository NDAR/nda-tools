from NDATools.DataManager import DataManager
import boto3


class S3Authentication:

    def __init__(self, config):
        self.config = config
        self.aws_session_token = None
        if config.aws_secret_key == "" and config.aws_access_key == "":
            data_manager_credentials = DataManager(config).get_data_manager_credentials
            self.aws_access_key = data_manager_credentials.get('aws_access_key_id')
            self.aws_secret_key =  data_manager_credentials.get('aws_secret_access_key')
            self.aws_session_token = data_manager_credentials.get('aws_session_token')
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

    @staticmethod
    def get_s3_client_with_config(aws_access_key, aws_secret_key, aws_session_token):
        return boto3.session.Session(aws_access_key_id=aws_access_key,
                                     aws_secret_access_key=aws_secret_key,
                                     aws_session_token=aws_session_token,
                                     region_name='us-east-1').client('s3')

    def get_s3_resource(self, s3_config):
        return boto3.Session(**self.credentials).resource('s3', config=s3_config)
