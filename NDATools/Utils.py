from __future__ import absolute_import, with_statement
from email.policy import HTTP

import json
import logging
import os
import re
import signal
import sys
import threading
import time
import traceback
import random 
import requests
from requests.adapters import HTTPAdapter
import requests.packages.urllib3.util

import NDATools
from NDATools.Configuration import ClientConfiguration

IS_PY2 = sys.version_info < (3, 0)

if IS_PY2:
    from io import open
    from urlparse import urlparse
else:
    from urllib.parse import urlparse

if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
    config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'))
else:
    config = ClientConfiguration('clientscripts/config/settings.cfg')

log_file = os.path.join(NDATools.NDA_TOOLS_VTCMD_LOGS_FOLDER, "debug_log_{}.txt").format(time.strftime("%Y%m%dT%H%M%S"))
# TODO - make the log level configurable by command line arg or env. variable. NDA_TOOLS_LOG_LEVEL=?
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")

if sys.version_info[0] < 3:
    input = raw_input
    import thread
else:
    import _thread as thread


class Protocol(object):
    CSV = "csv"
    XML = "xml"
    JSON = "json"

    @staticmethod
    def get_protocol(cls):
        return cls.JSON


def handle_http_error(r):
    # handle json and plain-text errors
    try:
        if 'json' in r.headers['Content-Type']:
            message = r.json()['message']
    except (ValueError, json.JSONDecodeError):
        message = r.text

    if r.status_code == 401:
        #provide default message if one doesnt already exist
        message = message or 'The NDA username or password is not recognized.'
    print()
    print(message)
    logging.error(message)
    print()
    r.raise_for_status()


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
        if data is not None:
            data = data.encode('utf-8')
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

    if not r.ok:
        handle_http_error(r)

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
    #TODO - correct regex. All single drive letters should be valid, (not just 'D'). Backslashes as file-separaters need to be added
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
    return str(round(bytes,2)) + units[0] if bytes < 1024 else human_size(bytes / 1024, units[1:])

def get_request(url,headers=None,auth=None, _json=None):
    tmp=None
    for i in range(10):
        try:
            with requests.Session() as session:
                session.mount(url,HTTPAdapter(max_retries=10))
                tmp = session.get(url,headers=headers,auth=auth,json=_json)
                tmp.raise_for_status()
            return tmp
        except requests.exceptions.ConnectionError as e:
            if i == 9:
                raise e
            time.sleep(random.randint(10,30))
    
def post_request(url,_json,headers=None,auth=None):
    tmp=None
    for i in range(10):
        try:
            with requests.Session() as session:
                session.mount(url,HTTPAdapter(max_retries=10))
                tmp = session.post(url,json=_json,headers=headers,auth=auth)
                tmp.raise_for_status()
            return tmp
        except requests.exceptions.ConnectionError as e:
            if i == 9:
                raise e
            time.sleep(random.randint(10,30))
        