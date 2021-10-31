from __future__ import absolute_import, with_statement

import concurrent
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor
from enum import Enum
import json as json_lib
import logging
import os
import re
import signal
import sys
import threading
import time
import traceback

import requests
from requests.adapters import HTTPAdapter
import requests.packages.urllib3.util

from NDATools.s3.S3Authentication import S3Authentication
from NDATools.vtmcd.SubmissionFile import SubmissionFile

try:
    from inspect import signature
except:
    from funcsigs import signature

import NDATools
from NDATools.vtmcd.Configuration import ClientConfiguration

IS_PY2 = sys.version_info < (3, 0)

if IS_PY2:
    from io import open
    from urlparse import urlparse
else:
    from urllib.parse import urlparse

if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
    config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
else:
    config = ClientConfiguration('../clientscripts/config/settings.cfg')

log_file = os.path.join(NDATools.NDA_TOOLS_VTCMD_LOGS_FOLDER, "debug_log_{}.txt").format(time.strftime("%Y%m%dT%H%M%S"))
# TODO - make the log level configurable by command line arg or env. variable. NDA_VTCMD_LOG_LEVEL=?
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")

if sys.version_info[0] < 3:
    input = raw_input
else:
    pass


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


def print_http_error_response(req, response):
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
                     retry_codes=[504], error_consumer=print_http_error_response, timeout=300, username=None,
                     password=None,
                     path_params=None, query_params=None):
    param_err_msg_template = '{} parameter is not of type {}'

    if endpoint and not isinstance(endpoint, str):
        raise TypeError(param_err_msg_template.format('endpoint', 'str'))

    if verb and not isinstance(verb, Verb):
        raise TypeError(param_err_msg_template.format('verb', 'Verb'))

    if headers is None:
        headers = {
            'Content-type': content_type.value,
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, compress, br, deflate'
        }

    if content_type and not isinstance(content_type, ContentType):
        raise TypeError(param_err_msg_template.format('content_type', 'ContentType'))

    if num_retry and not isinstance(num_retry, int):
        raise TypeError(param_err_msg_template.format('num_retry', 'int'))

    if retry_codes and not isinstance(retry_codes, list):
        raise TypeError(param_err_msg_template.format('retry_codes', 'list'))

    if error_consumer and not callable(error_consumer):
        raise TypeError('error_consumer is not callable')

    if timeout and not isinstance(timeout, int):
        raise TypeError(param_err_msg_template.format('timeout', 'int'))

    if username and not isinstance(username, str):
        raise TypeError(param_err_msg_template.format('username', 'str'))

    if password and not isinstance(password, str):
        raise TypeError(param_err_msg_template.format('password', 'str'))

    if headers and not isinstance(headers, dict):
        raise TypeError(param_err_msg_template.format('headers', 'dict'))

    if path_params and not isinstance(path_params, list):
        raise TypeError(param_err_msg_template.format('path_params', 'list'))

    if query_params and not isinstance(query_params, dict):
        raise TypeError(param_err_msg_template.format('query_params', 'dict'))

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

    response = None
    # method=None, url=None, headers=None, files=None, data=None, params=None, auth=None, cookies=None, hooks=None, json=None

    req_params = {
        'method': verb.name,
        'url': endpoint,
        'headers': headers,
        'auth': auth
    }
    if query_params:
        req_params['params'] = query_params

    if content_type is ContentType.JSON and data:
        if isinstance(data, str):
            req_params['data'] = data
        else:
            req_params['json'] = data
    elif data:
        req_params['data'] = data

    try:
        request = requests.Request(**req_params).prepare()
        response = session.send(request, timeout=timeout, stream=False)
    except requests.exceptions.RequestException as e:
        print('\nAn error occurred while making {} request, check your endpoint configuration:\n'.
              format(e.request.method))
        exit_client(signal.SIGTERM)

    response.raise_for_status()

    try:
        result = json_lib.loads(response.text)
    except ValueError:
        result = response.text

    if response.ok:
        return result

    error_consumer(response, result)


def get_error():
    exc_type, exc_value, exc_tb = sys.exc_info()
    tbe = traceback.TracebackException(
        exc_type, exc_value, exc_tb,
    )
    ex = ''.join(tbe.format_exception_only())
    # print('Error: {}'.format(ex))
    return ex


def get_stack_trace():
    exc_type, exc_value, exc_tb = sys.exc_info()
    tbe = traceback.TracebackException(
        exc_type, exc_value, exc_tb,
    )
    tb = ''.join(tbe.format())
    return tb
    # print(tb)


def is_valid_json(test_json):
    try:
        json_lib.loads(test_json)
        return True
    except:
        return False


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
    sys.exit(1)


def recollect_file_search_info():
    retry = input('Press the "Enter" key to specify directory/directories OR an s3 location by entering -s3 '
                  '<bucket name> to locate your associated files:')
    response = retry.split(' ')
    if response[0] == '-s3':
        source_bucket = response[1]
        source_prefix = input('Enter any prefix for your S3 object, or hit "Enter": ')
        return None, source_bucket, source_prefix
    else:
        return response, None, None


'''
    Returns a set of SubmissionFile objects, initiated with abs-path, size and s3 url
'''
def local_file_search(directory_list, associated_file_names):
    files = {SubmissionFile(a) for a in associated_file_names}
    # if not provided, default to working directory
    if not directory_list:
        directory_list = ['.']

    for f in files:
        f.find_and_set_local_file_info(directory_list)
    not_found = set(filter(lambda x: not x.abs_path, files))

    while not_found:
        message = 'You must make sure all associated files listed in your validation file' \
                  ' are located in the specified directory or AWS bucket. Associated file not found in specified directory:\n'
        print('\n', message)
        for file in not_found:
            print(file.csv_path)
        directory_list, bucket, prefix = recollect_file_search_info()
        for f in not_found:
            f.find_and_set_local_file_info(directory_list)
        not_found = set(filter(lambda x: not x.abs_path, not_found))
    # TODO - raise error if any files are missing read access
    return files

'''
    Returns a set of SubmissionFile objects, initiated with abs-path, size and s3 url
'''
def s3_file_search(ak, sk, bucket, prefix, associated_file_names):
    client = S3Authentication.get_s3_client_with_config(aws_access_key=ak, aws_secret_key=sk)
    files = {SubmissionFile(a) for a in associated_file_names}
    for f in files:
        f.find_and_set_s3_file_info(client, bucket, prefix, associated_file_names)
    not_found = set(filter(lambda x: not x.abs_path, files))

    while not_found:
        message = 'You must make sure all associated files listed in your validation file' \
                  ' are located in the specified directory or AWS bucket. Associated file not found in specified directory:\n'
        print('\n', message)
        for file in not_found:
            print(file.csv_path)
        directory_list, bucket, prefix = recollect_file_search_info()
        for f in not_found:
            f.find_and_set_s3_file_info(client, bucket, prefix, associated_file_names)
        not_found = set(filter(lambda x: not x.abs_path, not_found))
    # TODO - raise error if any files are missing read access
    return files

def sanitize_file_path(file):
    """
    Replaces backslashes with forward slashes and removes leading / or drive:/.

    :param file: Relative or absolute filepath
    :return: Relative or absolute filepath with leading / or drive:/ removed
    """
    # Sanitize all backslashes (\) with forward slashes (/)
    if file.startswith('s3://'):
        return file
    file_key = file.replace('\\', '/').replace('//', '/')
    # If Mac/Linux full path
    if re.search(r'^/.+$', file):
        file_key = file.split('/', 1)[1]
    # If Windows full path
    # TODO - correct regex. All single drive letters should be valid, (not just 'D'). Backslashes as file-separaters need to be added
    elif re.search(r'^\D:/.+$', file_key):
        file_key = file_key.split(':/', 1)[1]
    return file_key

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


# return bucket and key for url (handles http and s3 protocol)
def deconstruct_s3_url(url):
    tmp = urlparse(url)
    if tmp.scheme == 's3':
        bucket = tmp.netloc
        path = tmp.path.lstrip('/')
    elif tmp.scheme == 'https':
        # presigned urls are either https://bucketname.s3.amazonaws.com/key... or
        # https://s3.amazonaws.com/bucket/path
        if tmp.hostname == 's3.amazonaws.com':
            bucket = tmp.path.split('/')[1]
            path = '/'.join(tmp.path.split('/')[2:])
            path = '/' + path
        else:
            bucket = tmp.hostname.replace('.s3.amazonaws.com', '')
            path = tmp.path
    else:
        raise Exception('Invalid URL passed to deconstruct_s3_url method: {}'.format(url))

    return bucket, path.lstrip('/')


# converts . and .. and ~ in file-paths. (as well as variable names like %HOME%
def convert_to_abs_path(file_name):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(file_name)))


def human_size(bytes, units=[' bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']):
    """ Returns a human readable string representation of bytes """
    return str(round(bytes, 2)) + units[0] if bytes < 1024 else human_size(bytes / 1024, units[1:])

def flat_map(list_of_lists):
    return sum(list_of_lists, [])