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

        print()
        print('------ Mindar Created ------')
        print('Mindar ID: ' + str(response['mindar_id']))
        print('Package ID: ' + str(response['package_id']))
        print('Package Name: ' + str(response['name']))
        print('Mindar Schema: ' + str(response['schema']))
        print('Current Status: ' + str(response['status']))

    def show_mindars(self):
        response, session = api_request(self, "GET", self.__make_url())

        if len(response) <= 0:
            print('This user has no mindars, you can create one by executing \'mindar create\'.')
            return

        print('Showing ' + str(len(response)) + ' mindars...')
        print()
        print('Name,Schema,Mindar Id,Package Id,Status,Created Date')

        for mindar in response:
            print(f"{mindar['name']},{mindar['schema']},{mindar['mindar_id']},{mindar['package_id']},{mindar['status']},{mindar['created_date']}")
