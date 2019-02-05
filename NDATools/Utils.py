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
import signal

if sys.version_info[0] < 3:
    import ConfigParser as configparser

    input = raw_input
    import thread
else:
    import configparser
    import _thread as thread

import xml.etree.ElementTree as ET

class Protocol(object):
    CSV = "csv"
    XML = "xml"
    JSON = "json"

    @staticmethod
    def get_protocol(cls):
        return cls.JSON

def api_request(api, verb, endpoint, data=None, session=None):

    retry = requests.packages.urllib3.util.retry.Retry(
        total=20,
        read=20,
        connect=20,
        backoff_factor=3,
        status_forcelist=(400, 403, 404, 500, 502, 504)
    )

    headers = {'accept': 'application/json'}
    auth = None
    if isinstance(api, Protocol):
        if api.get_protocol(api) == Protocol.CSV:
            headers.update({'content-type': 'text/csv'})
        elif api.get_protocol(api) == Protocol.XML:
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
        exit_client(signal.SIGTERM)

    if r and r.ok:
        if api.__class__.__name__ == 'Download':
            return r, session
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
                    raise Exception(ValueError)

    if r.status_code == 401:
        m = 'The NDA username or password is not recognized.'
        print(m)
        r.raise_for_status()

    elif r.status_code == 400:
        response = json.loads(r.text)
        m = response['error'] + ': ' + response['message']
        print(m)
        r.raise_for_status()

    elif r.status_code == 500:
        response = r.text
        m = response
        print(m)
        r.raise_for_status()

    return response, session

def exit_client(signal, frame=None, message=None):
    for t in threading.enumerate():
        try:
            t.shutdown_flag.set()
        except AttributeError as e:
            continue
    if message:
        print('\n\n{}'.format(message))
    else:
        print('\n\nExit signal received, shutting down...')
    print('Please contact NDAHelp@mail.nih.gov if you need assistance.')
    sys.exit(1)

