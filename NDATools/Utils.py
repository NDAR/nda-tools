from __future__ import with_statement
from __future__ import absolute_import
import re
import signal
import sys
import getpass
import time
import threading
import traceback

import requests
from requests.adapters import HTTPAdapter
import requests.packages.urllib3.util
import json
import signal
import os
import logging

from NDATools.Configuration import ClientConfiguration

if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
    config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
else:
    config = ClientConfiguration('clientscripts/config/settings.cfg')
validation_results_dir = os.path.join(os.path.expanduser('~'), config.validation_results)
if not os.path.exists(validation_results_dir):
    os.mkdir(validation_results_dir)
log_file = os.path.join(validation_results_dir, "debug_log_{}.txt").format(time.strftime("%Y%m%dT%H%M%S"))
print('Opening log: {}'.format(log_file))
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")

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
        logging.error(e)
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
                logging.error(ValueError)
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
        logging.error(m)
        r.raise_for_status()

    elif r.status_code == 400:
        response = json.loads(r.text)
        m = response['error'] + ': ' + response['message']
        print(m)
        logging.error(m)
        r.raise_for_status()

    elif r.status_code in (500, 502, 503, 504):
        response = r.text
        m = response
        print(m)
        logging.error(m)
        r.raise_for_status()

    return response, session


def exit_client(signal=signal.SIGTERM, frame=None, message=None):
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


def parse_local_files(directory_list, no_match, full_file_path, no_read_access, skip_local_file_check):
    """
    Iterates through associated files generate a dictionary of full filepaths and file sizes.

    :param directory_list: List of directories
    :param no_match: Stores list of invalid paths
    :param full_file_path: Dictionary of tuples that store full filepath and file size
    :param no_read_access: List of files that user does not have access to
    :return: Modifies references to no_match, full_file_path, no_read_access
    """
    files_to_match = len(no_match)
    logging.debug('Starting local directory search for {} files'.format(str(files_to_match)))
    progress_counter = int(files_to_match * 0.05)
    for file in no_match[:]:
        if progress_counter == 0:
            logging.debug('Found {} files out of {}'.format(str(files_to_match - len(no_match)), str(files_to_match)))
            progress_counter = int(files_to_match * 0.05)
        file_key = sanitize_file_path(file)
        for d in directory_list:
            if skip_local_file_check:
                file_name = os.path.join(d, file)
                try:
                    full_file_path[file_key] = (file_name, os.path.getsize(file_name))
                    no_match.remove(file)
                    progress_counter -= 1
                except (OSError, IOError) as err:
                    if err.errno == 13:
                        print('Permission Denied: {}'.format(file_name))
                    continue
                break
            else:
                if os.path.isfile(file):
                    file_name = file
                elif os.path.isfile(os.path.join(d, file)):
                    file_name = os.path.join(d, file)
                else:
                    continue
                if not check_read_permissions(file_name):
                    no_read_access.add(file_name)
                full_file_path[file_key] = (file_name, os.path.getsize(file_name))
                no_match.remove(file)
                break
    logging.debug(
        'Local directory search complete, found {} files out of {}'.format(str(files_to_match - len(no_match)),
                                                                           str(files_to_match)))


def sanitize_file_path(file):
    """
    Replaces backslashes with forward slashes and removes leading / or drive:/.

    :param file: Relative or absolute filepath
    :return: Relative or absolute filepath with leading / or drive:/ removed
    """
    # Sanitize all backslashes (\) with forward slashes (/)
    file_key = file.replace('\\', '/').replace('//', '/')
    # If Mac/Linux full path
    if re.search(r'^/.+$', file):
        file_key = file.split('/', 1)[1]
    # If Windows full path
    elif re.search(r'^\D:/.+$', file_key):
        file_key = file_key.split(':/', 1)[1]
    return file_key


def check_read_permissions(file):
    try:
        open(file)
        return True
    except (OSError, IOError) as err:
        if err.errno == 13:
            print('Permission Denied: {}'.format(file))
    return False


def get_error():
    exc_type, exc_value, exc_tb = sys.exc_info()
    tbe = traceback.TracebackException(
        exc_type, exc_value, exc_tb,
    )
    ex = ''.join(tbe.format_exception_only())
    return 'Error: {}'.format(ex)


def get_traceback():
    exc_type, exc_value, exc_tb = sys.exc_info()
    tbe = traceback.TracebackException(
        exc_type, exc_value, exc_tb,
    )
    tb = ''.join(tbe.format())
    return tb
