from NDATools.Utils import *


class MindarManager:

    def __init__(self, config):
        self.config = config
        self.url = config.mindar

    def __make_url(self, extension=''):
        return self.url + extension

    def __authenticated_request(self, endpoint, **kwargs):
        return advanced_request(endpoint=endpoint, username=self.config.username, password=self.config.password, **kwargs)

    def create_mindar(self, password, nickname, package_id=None):
        payload = {
            'password': password
        }

        if package_id:
            payload['package_id'] = package_id

        if nickname:
            payload['nick_name'] = nickname

        return self.__authenticated_request(self.__make_url(), verb=Verb.POST, data=payload)

    def show_mindars(self, include_deleted=False):
        query_params = {}

        if include_deleted:
            query_params['excludeDeleted'] = 'false'

        return self.__authenticated_request(self.__make_url(), query_params=query_params)

    def delete_mindar(self, schema):
        return self.__authenticated_request(self.__make_url('/{}/'), path_params=[schema], verb=Verb.DELETE)

    def add_table(self, schema, table_name):
        return self.__authenticated_request(self.__make_url('/{}/tables'), path_params=[schema],
                                            query_params={'table_name': table_name}, verb=Verb.POST)

    def drop_table(self, schema, table_name):
        return self.__authenticated_request(self.__make_url('/{}/tables/{}/'), path_params=[schema, table_name],
                                            verb=Verb.DELETE)

    def show_tables(self, schema):
        return self.__authenticated_request(self.__make_url('/{}/tables/'), path_params=[schema])

    def refresh_stats(self, schema):
        return self.__authenticated_request(self.__make_url('/{}/refresh_stats'), path_params=[schema], verb=Verb.POST)

    def import_data_csv(self, schema, table_name, csv_data):
        print('Sending:')
        print(csv_data)

        return self.__authenticated_request(self.__make_url('/{}/tables/{}/records/'), path_params=[schema, table_name],
                                            content_type=ContentType.CSV, verb=Verb.POST, data=csv_data)
