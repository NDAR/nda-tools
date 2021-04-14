from NDATools.Utils import *


class MindarManager:

    def __init__(self, config):
        self.config = config
        self.url = config.mindar

    def __make_url(self, extension='/'):
        return self.url + extension

    def create_mindar(self, package_id, password, nickname):
        print('Creating a mindar...')

        payload = {
            'password': password
        }

        if package_id:
            print('Attaching package id')
            payload['package_id'] = package_id

        if nickname:
            print('Attaching nickname')
            payload['nick_name'] = nickname

        print('Making request')
        response, session = api_request(self, "POST", self.__make_url(), json=payload)

        print(response)
