from __future__ import with_statement
from __future__ import absolute_import
import signal
import sys
import getpass
import time
import threading
import requests
from requests.adapters import HTTPAdapter
import requests.packages.urllib3.util
import json
if sys.version_info[0] < 3:
    import ConfigParser as configparser
    input = raw_input
    import thread
else:
    import configparser
    import _thread as thread

import xml.etree.ElementTree as ET
import os

from pkg_resources import resource_filename


class ClientConfiguration:
    def __init__(self, settings_file):
        self.config = configparser.ConfigParser()
        if settings_file == os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'):
            config_location = settings_file
        else:
            config_location = resource_filename(__name__, settings_file)
        self.config.read(config_location)
        self.validation_api = self.config.get("Endpoints", "validation")
        self.submission_package_api = self.config.get("Endpoints", "submission_package")
        self.submission_api = self.config.get("Endpoints", "submission")
        self.validationtool_api = self.config.get("Endpoints", "validationtool")
        self.datamanager_api = self.config.get("Endpoints", "data_manager")
        self.validation_results = self.config.get("Files", "validation_results")
        self.submission_packages = self.config.get("Files", "submission_packages")
        self.collection_id = None
        self.endpoint_title = None
        self.scope = None
        self.directory_list = None
        self.manifest_path = None
        self.aws_access_key = self.config.get("User", "access_key")
        self.aws_secret_key = self.config.get("User", "secret_key")
        self.source_bucket = None
        self.source_prefix = None
        self.title = None
        self.description = None
        self.JSON = False
        self.username = self.config.get("User", "username")
        self.password = self.config.get("User", "password")


    def nda_login(self):
        if not self.username:
            self.username = input('Enter your NIMH Data Archives username:')

        if not self.password:
            self.password = getpass.getpass('Enter your NIMH Data Archives password:')


def exit_client(signal, frame=None, message=None):
    for thread in threading.enumerate():
        try:
            thread.shutdown_flag.set()
        except AttributeError:
            continue
    if message:
        print('\n\n{}'.format(message))
    else:
        print('\n\nExit signal received, shutting down...')
    print('Please contact NDAHelp@mail.nih.gov if you need assistance.')
    sys.exit(1)


def api_request(api, verb, endpoint, data=None, session=None):
    t1 = time.time()
    retry = requests.packages.urllib3.util.retry.Retry(
        total=20,
        read=20,
        connect=20,
        backoff_factor=3,
        status_forcelist=(400, 403, 404, 500, 502, 504)
    )

    headers = {'accept': 'application/json'}
    if api.__class__.__name__ == 'ValidationTask':
        auth = None
        headers.update({'content-type': 'text/csv'})
    elif api.__class__.__name__ == 'Download':
        auth = None
        headers.update({'content-type': 'text/xml'})
    else:
        auth = requests.auth.HTTPBasicAuth(api.config.username, api.config.password)
        headers.update({'content-type': 'application/json'})
    if not session:
        session = requests.session()
        session.mount(endpoint, HTTPAdapter(max_retries=retry))
    r = None
    response = None
    try:
        r = session.send(requests.Request(verb, endpoint, headers, auth=auth, data=data).prepare(),
                         timeout=300, stream=False)
    except requests.exceptions.RequestException as e:
        print('\nAn error occurred while making {} request, check your endpoint configuration:\n'.
              format(e.request.method))
        print(e)
        if api.__class__.__name__.endswith('Task'):
            api.shutdown_flag.set()
            thread.interrupt_main()
        exit_client(signal.SIGINT)

    if r:
        if r.ok:
            if api.__class__.__name__ == 'Download':
                package_list = []
                root = ET.fromstring(r.text)
                path = root.findall(".//path")
                for element in path:
                    file = 's3:/' + element.text
                    package_list.append(file)
                return package_list
            else:
                try:
                    response = json.loads(r.text)
                except ValueError:
                    print('Your request returned an unexpected response, please check your endpoints.\n'
                          'Action: {}\n'
                          'Endpoint:{}\n'
                          'Status:{}\n'
                          'Reason:{}'.format(verb, endpoint, r.status_code, r.reason))
                    if api.__class__.__name__.endswith('Task'):
                        api.shutdown_flag.set()
                        thread.interrupt_main()
                    else:
                        exit_client(signal.SIGINT)
        elif r.status_code == 401:
            tries = 0
            while r.status_code == 401 and tries < 5:
                print('The username or password is not recognized.')
                username=input('Please enter your username:')
                password=getpass.getpass('Please enter your password:')
                auth = requests.auth.HTTPBasicAuth(username, password)
                r = session.send(requests.Request(verb, endpoint, headers, auth=auth, data=data).prepare(),
                                 timeout=300, stream=False)
                tries += 1
            if r.ok:
                response = json.loads(r.text)
                # print('Authentication successful, updating username/password.')
                api.username=username
                api.config.username=username
                api.password=password
                api.config.password=password
            else:
                exit_client(signal.SIGINT, message='Too many unsuccessful authentication attempts.')

    if r.status_code == 400:
        response = json.loads(r.text)
        m = response['error'] + ': ' + response['message']
        print(m)
        #exit_client(signal.SIGINT, message=m)
        r.raise_for_status()

    if r.status_code == 500:
        response = json.loads(r.text)
        m = response['error'] + ': ' +  response['message']
        print(m)
        r.raise_for_status()
        #exit_client(signal.SIGINT, message=m)

    return response, session
