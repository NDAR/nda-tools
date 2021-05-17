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
import json as json_lib
import signal
import os
import logging

from enum import Enum
try:
    from inspect import signature
except:
    from funcsigs import signature

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


class ContentType(Enum):
    CSV = "text/csv"
    XML = "text/xml"
    JSON = "application/json"


class Verb(Enum):
    GET = 1
    POST = 2
    PUT = 3
    DELETE = 4
    UPDATE = 5


def report_error(req, response):
    if 'error' not in response and 'message' not in response:
        message = 'Error response from server: {}'.format(response)
    else:
        e = response['error'] if 'error' in response else 'An error occurred during processing of the last request'
        m = response['message'] if 'message' in response else 'No Error Message Available'
        message = '{}: {}'.format(e, m)

    print(message)


# This method was written as a replacement to api_request, it has been expanded to try and support as many
# possible use-cases to make it an extremely versatile backbone for the client's HTTP requesting requirements
def advanced_request(endpoint, verb=Verb.GET, content_type=ContentType.JSON, data=None, headers=None, num_retry=0,
                     retry_codes=None, error_consumer=report_error, timeout=300, username=None, password=None,
                     raise_on_error=True, path_params=None, query_params=None):

    if retry_codes is None:
        retry_codes = [504]

    error_msg = '{} parameter is not of type {}'

    if endpoint and not isinstance(endpoint, str):
        raise TypeError(error_msg.format('endpoint', 'str'))

    if verb and not isinstance(verb, Verb):
        raise TypeError(error_msg.format('verb', 'Verb'))

    if headers is None:
        headers = {
            'content-type': content_type.value
        }

    if content_type and not isinstance(content_type, ContentType):
        raise TypeError(error_msg.format('content_type', 'ContentType'))

    if num_retry and not isinstance(num_retry, int):
        raise TypeError(error_msg.format('num_retry', 'int'))

    if retry_codes and not isinstance(retry_codes, list):
        raise TypeError(error_msg.format('retry_codes', 'list'))

    if error_consumer and not callable(error_consumer):
        raise TypeError('error_consumer is not callable')

    if timeout and not isinstance(timeout, int):
        raise TypeError(error_msg.format('timeout', 'int'))

    if username and not isinstance(username, str):
        raise TypeError(error_msg.format('username', 'str'))

    if password and not isinstance(password, str):
        raise TypeError(error_msg.format('password', 'str'))

    if headers and not isinstance(headers, dict):
        raise TypeError(error_msg.format('headers', 'dict'))

    if raise_on_error and not isinstance(raise_on_error, bool):
        raise TypeError(error_msg.format('raise_on_error', 'bool'))

    if path_params and not isinstance(path_params, list):
        raise TypeError(error_msg.format('path_params', 'list'))

    if query_params and not isinstance(query_params, dict):
        raise TypeError(error_msg.format('query_params', 'dict'))

    # This method is written like this because we don't want this error to
    # present if there is no authentication data supplied
    if (username and not password) or (password and not username):
        raise ValueError('username AND password are required')

    if path_params:
        for p in path_params:
            if not p or not isinstance(p, str):
                raise TypeError('path_params must be list of type str with no Nones')

    if query_params:
        for n, q in query_params.items():
            if not n or not isinstance(n, str):
                raise TypeError('query_params must be dict of type str with no Nones')

            if not q or not isinstance(q, str):
                raise TypeError('query_params must be dict of type str with no Nones')

    if retry_codes:
        for c in retry_codes:
            if not c or not isinstance(c, int):
                raise TypeError('retry_codes must be list of type int with no Nones')

    if path_params:
        endpoint = endpoint.format(*path_params)

    appended_query_params = []

    if query_params:
        if endpoint.endswith('/'):
            endpoint = endpoint.removesuffix('/')

        for name, value in query_params.items():
            appended_query_params.append(name + '=' + value)
    elif not endpoint.endswith('/'):
        endpoint += '/'

    if appended_query_params:
        endpoint += '?' + '&'.join(appended_query_params)

    if content_type is ContentType.JSON and data:
        try:
            data = json_lib.dumps(data)
        except ValueError:
            raise TypeError('data cannot be converted to json object')

    retry = None

    if num_retry and retry_codes:
        retry = requests.packages.urllib3.util.retry.Retry(total=num_retry, status_forcelist=retry_codes)

    if 'content-type' not in headers:
        headers['content-type'] = content_type.value

    auth = None

    # We know that if username is present password will be present too due to error checking
    if username:
        auth = requests.auth.HTTPBasicAuth(username, password)

    session = requests.session()

    if retry:
        session.mount(endpoint, HTTPAdapter(max_retries=retry))
    else:
        session.mount(endpoint, HTTPAdapter())

    sig = signature(error_consumer)

    if len(sig.parameters) != 2:
        raise ValueError('error_consumer requires 2 parameters, but has {}'.format(len(sig.parameters)))

    req = None

    try:
        req = session.send(requests.Request(verb.name, endpoint, headers, auth=auth, data=data).prepare(), timeout=timeout, stream=False)
    except requests.exceptions.RequestException as e:
        print('\nAn error occurred while making {} request, check your endpoint configuration:\n'.
              format(e.request.method))
        exit_client(signal.SIGTERM)

    try:
        result = json_lib.loads(req.text)
    except ValueError:
        result = req.text

    if req.ok:
        return result

    error_consumer(req, result)

    if raise_on_error:
        req.raise_for_status()


def get_error():
    exc_type, exc_value, exc_tb = sys.exc_info()
    tbe = traceback.TracebackException(
        exc_type, exc_value, exc_tb,
    )
    ex = ''.join(tbe.format_exception_only())
    #print('Error: {}'.format(ex))
    return ex


def get_stack_trace():
    exc_type, exc_value, exc_tb = sys.exc_info()
    tbe = traceback.TracebackException(
        exc_type, exc_value, exc_tb,
    )
    tb = ''.join(tbe.format())
    return tb
    #print(tb)


def api_request(api, verb, endpoint, data=None, session=None, json=None):

    if data and json:
        raise Exception(ValueError)
    elif json:
        data = json_lib.dumps(json)

    retry_request = requests.packages.urllib3.util.retry.Retry(
        total=7,
        read=20,
        connect=20,
        backoff_factor=3,
        status_forcelist=(502, 504)
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
        session.mount(endpoint, HTTPAdapter(max_retries=retry_request))
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

    if r.ok:
        if api.__class__.__name__ == 'Download':
            return r, session
        else:
            try:
                response = json_lib.loads(r.text)
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

    elif r.status_code == 401:
        m = 'The NDA username or password is not recognized.'
        print(m)
        logging.error(m)
        r.raise_for_status()

    else:
        # default error message
        message ='Error occurred while processing request {} {}.\r\n'.format(verb, endpoint)
        message += 'Error response from server: {}'.format(r.text)

        if 'application/json' in r.headers['Content-Type'] and r.text:
            response = json_lib.loads(r.text)
            if 'error' not in response and 'message' not in response:
                message = 'Error response from server: {}'.format(response)
            else:
                e = response['error'] if 'error' in response else 'An error occurred during processing of the last request'
                m = response['message'] if 'message' in response else 'No Error Message Available'
                message = '{}: {}'.format(e, m)
        print(message)
        logging.error(message)
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
    progress_counter = int(files_to_match*0.05)
    for file in no_match[:]:
        if progress_counter == 0:
            logging.debug('Found {} files out of {}'.format(str(files_to_match - len(no_match)), str(files_to_match)))
            progress_counter = int(files_to_match*0.05)
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
    logging.debug('Local directory search complete, found {} files out of {}'.format(str(files_to_match - len(no_match)), str(files_to_match)))


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
