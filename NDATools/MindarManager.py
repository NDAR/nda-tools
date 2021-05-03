from NDATools.Utils import *


class MindarManager:

    def __init__(self, config):
        self.config = config
        self.url = config.mindar
        self.__csv_protocol = MindarCSVProtocolHack(config)

    def __make_url(self, extension='', query_params=None):
        params = []

        if isinstance(query_params, dict):
            if query_params:
                if extension.endswith('/'):
                    extension = extension.removesuffix('/')

                for name, value in query_params.items():
                    params.append(name + '=' + value)
        elif not extension.endswith('/'):
            extension += '/'

        url = self.url + extension

        if params:
            return url + '?' + '&'.join(params)

        return url

    def create_mindar(self, password, nickname, package_id=None):
        payload = {
            'password': password
        }

        if package_id:
            payload['package_id'] = package_id

        if nickname:
            payload['nick_name'] = nickname

        return request(self.__make_url(), verb=Verb.POST, data=payload, username=self.config.username, password=self.config.password)

        # response, session = api_request(self, "POST", self.__make_url(), json=payload)
        #
        # return response

    def show_mindars(self, include_deleted=False):
        query_params = {}

        if include_deleted:
            query_params['excludeDeleted'] = 'false'

        response, session = api_request(self, "GET", self.__make_url(query_params=query_params))

        return response

    def delete_mindar(self, schema):
        response, session = api_request(self, "DELETE", self.__make_url('/{}/'.format(schema)))

        return response

    def add_table(self, schema, table_name):
        response, session = api_request(self, "POST", self.__make_url('/{}/tables'.format(schema), {'table_name': table_name}))
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

    def import_data_csv(self, schema, table_name, csv_data):
        # TODO: This isn't possible using api_request, write a new version then use that

        response, session = api_request(self.__csv_protocol, "POST", self.__make_url('/{}/tables/{}/records/'.format(schema, table_name)), data=csv_data)

        return response


# TODO: This is technical debt, I opted to write this instead of rewriting api_request for time reasons
class MindarCSVProtocolHack (Protocol):

    def __init__(self, config):
        self.config = config

    @staticmethod
    def get_protocol(cls):
        return Protocol.CSV