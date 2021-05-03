from datetime import datetime
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

    def export_table_to_file(self, schema, table, root_dir='.', include_id=False):
        final_csv_dest = os.path.join(root_dir, '{}.csv'.format(table))

        if os.path.isfile(final_csv_dest):
            os.remove(final_csv_dest)

        try:
            with open(final_csv_dest, 'wb') as f:
                print('Exporting table {} to {}'.format(table, final_csv_dest))
                basic_auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
                headers = { 'Accept' : 'text/plain'}

                WAIT_TIME_SEC = 60 * 60 * .5
                s = requests.Session() # use of a session instead of a 'request' object directly avoids errors during read and automatically adds features like 'Keep-Alive'
                with s.get(self.__make_url('/{}/tables/{}/records?include_table_row_id={}'.format(schema, table, include_id)), stream=True, auth=basic_auth, headers=headers, timeout=WAIT_TIME_SEC) as r:
                    if not r.ok:
                        r.raise_for_status()
                    line_count = 0
                    PROGRESS_REPORT_INTERVAL = 10000
                    for line in r.iter_lines():
                        line_count += 1
                        # filter out keep-alive new lines
                        if (line_count - 1 ) % PROGRESS_REPORT_INTERVAL == 0:
                            print(
                                'Exporting {} {} rows (currently at row #{})'.format('first' if line_count==1 else 'next', PROGRESS_REPORT_INTERVAL, line_count))
                        if line:
                            f.write(line)
                            f.write(b"\n")

                f.flush()
        finally:
            if s:
                s.close()

