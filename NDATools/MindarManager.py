import requests
import re
import os
import logging

from datetime import datetime
from NDATools.Utils import advanced_request, Verb, ContentType, get_stack_trace


__all__ = ['MindarManager']


class MindarManager:

    def __init__(self, config):
        self.config = config
        self.url = config.mindar
        self.session = requests.Session()
        a = requests.adapters.HTTPAdapter(max_retries=requests.packages.urllib3.util.retry.Retry(total=6, status_forcelist=[502, 503], backoff_factor=3, read=300, connect=20))
        self.session.mount('https://', a)
        self.session.mount('http://', a)

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
        return self.__authenticated_request(self.__make_url('/{}/tables/{}/records/'), path_params=[schema, table_name],
                                            content_type=ContentType.CSV, verb=Verb.POST, data=csv_data)

    # TODO Refactor me!
    def export_table_to_file(self, schema, table, root_dir='.', include_id=False, add_nda_header=False):
        start = datetime.now()
        invalid_structure = False
        try:
            final_csv_dest = os.path.join(root_dir, '{}.csv'.format(table))

            if os.path.isfile(final_csv_dest):
                os.remove(final_csv_dest)

            with open(final_csv_dest, 'wb') as f:
                print('Exporting table {} to {}'.format(table, final_csv_dest))
                basic_auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
                self.session.headers['Accept'] = 'text/plain'

                WAIT_TIME_SEC = 60 * 60 * .5
                with self.session.get(self.__make_url('/{}/tables/{}/records?include_table_row_id={}'.format(schema, table, include_id)), stream=True, auth=basic_auth, timeout=WAIT_TIME_SEC) as r:
                    if not r.ok:
                        if r.status_code == 404 and 'Data-structure {} does not exist or does not correspond to a data structure'.format(table) in r.text:
                            invalid_structure = True

                        r.raise_for_status()
                    if add_nda_header:
                        version = re.search(r'^.*?(\d+)$', table).group(1)
                        name = table.rstrip(version)
                        f.write('{},{}\n'.format(name, version).encode("UTF-8"))
                    for content in r.iter_content(chunk_size=None):
                        if content:
                            f.write(content)

                f.flush()
                print('Done exporting table {} to {} at {}'.format(table, final_csv_dest, datetime.now()))

                return f.name

        except Exception as e:
            if invalid_structure:
                print('Error while trying to export table {}: Could not find corresponding data-structure in NDA.'
                      ' Only public data-structures can be exported in this iteration of the mindar tool.'.format(table, e))
            else:
                print('Error while trying to export table {}. Error was {}'.format(table, e))
                # for debugging
                print(get_stack_trace())
                logging.error(get_stack_trace())
                print('Export attempt took {}'.format(datetime.now() - start))
            raise e
