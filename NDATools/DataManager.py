from __future__ import with_statement
from __future__ import absolute_import

from NDATools.TokenGenerator import *

class DataManager:

    def __init__(self, config):
        self.config = config
        self.username = self.config.username
        self.password = self.config.password
        self.api = self.config.datamanager_api
        self.credentials = self.get_data_manager_credentials

    @property
    def get_data_manager_credentials(self):
        data_manager_token = NDATokenGenerator(self.api).generate_token(self.username,
                                                                        self.password)
        credentials = {'aws_access_key_id': data_manager_token.access_key,
                       'aws_secret_access_key': data_manager_token.secret_key,
                       'aws_session_token': data_manager_token.session}
        return credentials