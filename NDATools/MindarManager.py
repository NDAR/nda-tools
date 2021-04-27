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
            payload['package_id'] = package_id

        if nickname:
            payload['nick_name'] = nickname

        response, session = api_request(self, "POST", self.__make_url(), json=payload)

        return response

    def show_mindars(self, include_deleted=False):

        query_params = []
        if (include_deleted):
            query_params.append(('excludeDeleted=false'))

        q = ''
        if query_params:
            q = '?' + '&'.join(query_params)
        response, session = api_request(self, "GET", self.__make_url(q))

        return response

    def delete_mindar(self, schema):
        response, session = api_request(self, "DELETE", self.__make_url(f'/{schema}/'))

        return response

    def add_table(self, schema, table_name):
        response, session = api_request(self, "POST", self.__make_url('/{}/tables?table_name={}'.format(schema, table_name)))
        return response


    def drop_table(self, schema, table_name):
        response, session = api_request(self, "DELETE", self.__make_url('/{}/tables/{}/'.format(schema, table_name)))
        return response


    def show_tables(self, schema):
        response, session = api_request(self, "GET", self.__make_url('/{}/tables/'.format(schema)))
        return response

    def refresh_stats(self, schema):
        response, session = api_request(self, "POST", self.__make_url('/{}/refresh_stats'.format(schema)))
        return response
