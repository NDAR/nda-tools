import http

from NDATools.Utils import *
import requests
import pandas as pd

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
        f_name_tmp = os.path.join(root_dir, '{}.csv.tmp'.format(table))
        final_csv_dest = f_name_tmp.replace('.tmp', '')

        with open(f_name_tmp, 'w') as f:
            print('Exporting table {} to {}'.format(table, final_csv_dest))
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
                    if (line_count - 1 ) % PROGRESS_REPORT_INTERVAL == 0:
                        print(
                            'Exporting {} {} rows (currently at row #{})'.format('first' if line_count==1 else 'next', PROGRESS_REPORT_INTERVAL, line_count))
                    if line:
                        f.write(line.decode('utf-8') + "\n")

            f.flush()

        if include_id:
            os.rename(f_name_tmp, final_csv_dest)
            return
        # continue processing file to get rid of ID column
        print('Removing ID column from csv {}'.format(final_csv_dest))

        f = pd.read_csv(f_name_tmp)
        f.drop('{}_ID'.format(table.upper()), inplace=True, axis=1)
        f.to_csv(final_csv_dest, encoding='utf-8',index=False)
        os.remove(f_name_tmp)