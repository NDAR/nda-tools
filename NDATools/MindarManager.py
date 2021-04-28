import http

from NDATools.Utils import *
import requests

class MindarManager:

    def __init__(self, config):
        self.config = config
        self.url = config.mindar

    def __make_url(self, extension='/'):
        return self.url + extension

    def create_mindar(self, password, nickname, package_id=None):
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

    def export_table_to_file(self, schema, table, root_dir='.'):
        with open(os.path.join(root_dir, '{}.csv'.format(table)), 'w') as f:
            print('Exporting table {} to {}'.format(table, f.name))
            basic_auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
            headers = { 'Accept' : 'text/plain'}

            with requests.get(self.__make_url('/{}/tables/{}/records'.format(schema, table)), stream=True, auth=basic_auth, headers=headers) as r:
                if not r.ok:
                    r.raise_for_status()
                line_count = 0
                PROGRESS_REPORT_INTERVAL = 10000
                for line in r.iter_lines():
                    line_count += 1
                    # filter out keep-alive new lines
                    if line_count % PROGRESS_REPORT_INTERVAL == 0:
                        print(
                            'Exporting row {} - {}'.format(line_count, line_count + (PROGRESS_REPORT_INTERVAL - 1)))
                    if line:
                        f.write(line.decode('utf-8') + "\n")

            f.flush()