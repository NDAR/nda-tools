from NDATools.Utils import *


class MindarManager:

    def __init__(self, config):
        self.config = config
        self.url = config.mindar

    def __make_url(self, extension='/'):
        return self.url + extension

    def create_mindar(self, package_id, password, nickname):
        payload = {
            'password': password
        }

        if package_id:
            print('Creating a mindar for package ' + str(package_id))
            payload['package_id'] = package_id
        else:
            print('Creating an empty mindar...')

        if nickname:
            payload['nick_name'] = nickname

        response, session = api_request(self, "POST", self.__make_url(), json=payload)

        print('')
        print('------ Mindar Created ------')
        print('Mindar ID: ' + str(response['mindar_id']))
        print('Package ID: ' + str(response['package_id']))
        print('Package Name: ' + str(response['name']))
        print('Mindar Schema: ' + str(response['schema']))
        print('Current Status: ' + str(response['status']))
