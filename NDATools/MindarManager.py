from NDATools.Utils import *


class MindarManager:

    def __init__(self, config):
        self.config = config
        self.url = config.mindar

    def __make_url(self, extension='/', replace=None):
        if replace:
            return self.url + extension.format(replace)

        return self.url + extension

    def create_mindar(self, package_id, password, nickname):
        payload = {
            'password': password
        }

        if package_id:
            payload['package_id'] = package_id

        if nickname:
            payload['nick_name'] = nickname

        response, session = api_request(self, "POST", self.__make_url(), json=payload)

        return response

    def show_mindars(self):
        response, session = api_request(self, "GET", self.__make_url())

        return response

    def delete_mindar(self, schema):
        response, session = api_request(self, "DELETE", self.__make_url(f'/{schema}/'))

        return response

    def add_table(self, schema, table_name):
        payload = {
            'table_name': table_name
        }

        response, session = api_request(self, "POST", self.__make_url('/{}/tables', schema), json=payload)

        return response
