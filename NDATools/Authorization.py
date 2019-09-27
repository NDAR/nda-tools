from NDATools.DataManager import DataManager


class Authorization:

    def __init__(self, config):
        self.config = config
        if config.aws_secret_key == "" and config.aws_access_key == "":
            self.aws_access_key, self.aws_secret_key, self.aws_session_token = DataManager(config).credentials
        else:
            self.aws_access_key = config.aws_access_key
            self.aws_secret_key = config.aws_secret_key
            if config.aws_session_token != "":
                self.aws_session_token = config.aws_session_token

    @property
    def get_credentials(self):
        return {'aws_access_key_id': self.aws_access_key,
                'aws_secret_access_key': self.aws_secret_key,
                'aws_session_token': self.aws_session_token}

    # def get_session:
        # Check if token is expired, if so, get new token

    # consider adding s3 sesssion s3client in here
