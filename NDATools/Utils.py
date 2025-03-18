import datetime
import functools
import json
import logging
import os
import random
import re
import sys
import threading
import time
import traceback
import urllib.parse
from pathlib import Path
from urllib.parse import urlparse

import boto3
import requests
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)


class Protocol(object):
    CSV = "csv"
    XML = "xml"
    JSON = "json"

    @staticmethod
    def get_protocol(cls):
        return cls.JSON


class DeserializeHandler():

    @staticmethod
    def none(r):
        return r

    @staticmethod
    def convert_json(r):
        return json.loads(r.text)


class HttpErrorHandlingStrategy():
    # error handling implementation methods. Each method takes a response object
    @staticmethod
    def ignore(r):
        pass

    @staticmethod
    def print_and_exit(r):
        # handle json and plain-text errors
        message = None

        if r.status_code == 503:
            message = 'Service temporarily unavailable'
        elif r.status_code == 401:
            message = 'Error authenticating the endpoint: incorrect NDA username or password'
        elif 'content-type' in r.headers and 'json' in r.headers['Content-Type']:
            try:
                message = r.json()
                if message.get('message', None):
                    message = message.get('message')
            except (ValueError, json.JSONDecodeError):
                message = r.text
        else:
            message = r.text

        if message is not None and len(str(message).strip()) > 0:
            logger.error(
                '\nAn unexpected error was encountered and the program could not continue. Error message from service was: \n%s' % message)
        else:
            logger.error('\nAn unexpected error was encountered and the program could not continue.\n')
        exit_error()

    @staticmethod
    def reraise_status(response):
        response.raise_for_status()


def _exit_client(message=None, status_code=1):
    for t in threading.enumerate():
        try:
            t.shutdown_flag.set()
        except AttributeError as e:
            continue
    if message:
        logger.info('\n\n{}'.format(message))
    else:
        logger.info('\n\nExit signal received, shutting down...')
    os._exit(status_code)


def exit_error(message=None):
    _exit_client(message, status_code=1)


def exit_normal(message=None):
    _exit_client(message, status_code=0)


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
    logger.debug('Starting local directory search for {} files'.format(str(files_to_match)))
    for file in no_match[:]:
        file_key = sanitize_file_path(file)
        for d in directory_list:
            if skip_local_file_check:
                file_name = os.path.join(d, file)
                try:
                    full_file_path[file_key] = (file_name, os.path.getsize(file_name))
                    no_match.remove(file)
                except (OSError, IOError):
                    full_file_path[file_key] = (file_name, None)
                    no_match.remove(file)
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
                sys.stdout.write(
                    '\rFound {} files out of {}\r'.format(str(files_to_match - len(no_match)), str(files_to_match)))
                sys.stdout.flush()
                break
    sys.stdout.write('{}\r'.format(' ' * 64))  # clear out 'Found {} files ...' message and reset cursor
    sys.stdout.flush()
    logger.debug(
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


def sanitize_windows_download_filename(filepath):
    forbidden_windows_chars = ['<', '>', ':', '|', '?', '*']
    drive, path = os.path.splitdrive(filepath)

    if any(char in path for char in forbidden_windows_chars):
        for char in forbidden_windows_chars:
            filepath = os.path.join(drive, str(filepath).replace(char, urllib.parse.quote(char)))
    return filepath


def check_read_permissions(file):
    try:
        open(file)
        return True
    except (OSError, IOError) as err:
        if err.errno == 13:
            logger.info('Permission Denied: {}'.format(file))
    return False


def evaluate_yes_no_input(message, default_input=None):
    while True:
        default_print = ' (Y/n)' if default_input.upper() == 'Y' else ' (y/N)' if default_input.upper() == 'N' else ''
        user_input = input('{}{}'.format(message, default_print)) or default_input
        if str(user_input).upper() in ['Y', 'N']:
            return user_input.lower()
        else:
            print('Input not recognized.')


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


def retry_connection_errors(func):
    @functools.wraps(func)
    def _retry(*args, **kwargs):
        tmp = None
        for i in range(10):
            try:
                tmp = func(*args, **kwargs)
                return tmp
            except requests.exceptions.ConnectionError as e:
                if i == 9:
                    raise e
                time.sleep(random.randint(10, 30))

    return _retry


def is_json(test):
    try:
        json.dumps(test)
        return True
    except:
        return False


@retry_connection_errors
def _send_prepared_request(prepped, timeout=150, deserialize_handler=DeserializeHandler.convert_json,
                           error_handler=HttpErrorHandlingStrategy.print_and_exit):
    with requests.Session() as session:
        retries = Retry(total=10,
                        backoff_factor=0.1,
                        status_forcelist=[502, 503, 504])
        logger.debug('{} {} @ {}'.format(prepped.method, prepped.url, datetime.datetime.now()))
        session.mount(prepped.url, HTTPAdapter(max_retries=retries))
        tmp = session.send(prepped, timeout=timeout)
        logger.debug(
            '{} {} (elapsed = {})- STATUS {}'.format(prepped.method, prepped.url, tmp.elapsed, tmp.status_code))
        if not tmp.ok:
            error_handler(tmp)
    return deserialize_handler(tmp)


def get_request(url, headers={}, auth=None, timeout=150, deserialize_handler=DeserializeHandler.convert_json,
                error_handler=HttpErrorHandlingStrategy.print_and_exit):
    req = requests.Request('GET', url, auth=auth, headers=headers)
    return _send_prepared_request(req.prepare(), timeout=timeout, deserialize_handler=deserialize_handler,
                                  error_handler=error_handler)


def post_request(url, payload=None, headers={}, auth=None, timeout=150,
                 deserialize_handler=DeserializeHandler.convert_json,
                 error_handler=HttpErrorHandlingStrategy.print_and_exit):
    data_param, headers = get_data_and_header_params(payload, headers)
    req = requests.Request('POST', url, auth=auth, headers=headers, **data_param)
    return _send_prepared_request(req.prepare(), timeout=timeout, deserialize_handler=deserialize_handler,
                                  error_handler=error_handler)


def put_request(url, payload=None, headers={}, auth=None, timeout=150,
                deserialize_handler=DeserializeHandler.convert_json,
                error_handler=HttpErrorHandlingStrategy.print_and_exit):
    data_param, headers = get_data_and_header_params(payload, headers)
    req = requests.Request('PUT', url, auth=auth, headers=headers, **data_param)
    return _send_prepared_request(req.prepare(), timeout=timeout, deserialize_handler=deserialize_handler,
                                  error_handler=error_handler)


def get_data_and_header_params(payload, headers):
    data_param = {}
    if 'content-type' not in headers:
        if isinstance(payload, dict) or isinstance(payload, list):
            # setting the json arg will automatically add the content-header
            # https://requests.readthedocs.io/en/latest/user/quickstart/#more-complicated-post-requests
            data_param = {'json': payload}
        else:
            data_param = {'data': payload}
            if isinstance(payload, str) and is_json(payload) and 'content-type' not in headers:
                headers['content-type'] = 'application/json'
    else:
        data_param = {'data': payload}
    return data_param, headers


def get_s3_client_with_config(aws_access_key, aws_secret_key, aws_session_token):
    return boto3.session.Session(aws_access_key_id=aws_access_key,
                                 aws_secret_access_key=aws_secret_key,
                                 aws_session_token=aws_session_token,
                                 region_name='us-east-1').client('s3')


def get_s3_resource(aws_access_key, aws_secret_key, aws_session_token, s3_config):
    return boto3.Session(aws_access_key_id=aws_access_key,
                         aws_secret_access_key=aws_secret_key,
                         aws_session_token=aws_session_token).resource('s3', config=s3_config)


def collect_directory_list():
    while True:
        retry = input('Press the "Enter" key to specify directory/directories OR an s3 location by entering -s3 '
                      '<bucket name> to locate your associated files:')
        response = retry.split(' ')
        directories = list(map(lambda x: Path(x.strip()), response))
        if all(map(lambda x: os.path.isdir(x), directories)):
            return list(map(lambda x: x.resolve(), directories))
        else:
            not_existent = list(filter(lambda x: not os.path.isdir(x), directories))
            logger.error(f"The following directories cannot be found:\n")
            for directory in not_existent:
                logger.error(f"\t{directory.resolve()}")


def get_non_blank_input(prompt, input_name):
    while True:
        user_input = input(prompt)
        if user_input.strip():
            return user_input
        else:
            print('{} cannot be blank. Please try again'.format(input_name))


def get_object(s3_url, /, access_key_id, secret_access_key, session_token):
    # split the s3_url to get a bucket and key
    bucket, key = s3_url.replace("s3://", "").split("/", 1)
    # use the boto3 client to get the results and display the contents
    try:
        result = boto3.client(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token
        ).get_object(Bucket=bucket, Key=key)
        # print(f"{key}: {result['Body'].read(1024).decode('utf-8')}")
        return result['Body'].read()
    except ClientError as e:
        print(f"An error occurred: {e}")
        exit(1)
