import datetime
import json
import logging
import os
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Callable, List, Tuple, Type
from urllib.parse import urlparse, unquote

import boto3
import requests
from pydantic import BaseModel
from requests.adapters import HTTPAdapter, Retry
from tqdm.contrib.concurrent import thread_map

from NDATools import exit_error

logger = logging.getLogger(__name__)

REQUEST_TOKEN_VERSION_ATTR = '_nda_token_version'
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
                response_json = r.json()
                if 'message' in response_json:
                    message = response_json['message']
            except (ValueError, json.JSONDecodeError):
                message = r.text
        else:
            message = r.text

        could_not_continue = 'An unexpected error was encountered and the program could not continue.'
        logger.error('\n%s' % could_not_continue)
        if message is not None and len(str(message).strip()) > 0:
            logger.error('\nError message from service was: \n%s' % message)
        exit_error()

    @staticmethod
    def reraise_status(response):
        response.raise_for_status()


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
            filepath = os.path.join(str(filepath).replace(char, urllib.parse.quote(char)))
    return filepath


def check_read_permissions(file):
    try:
        open(file)
        return True
    except (OSError, IOError) as err:
        if err.errno == 13:
            logger.info('Permission Denied: {}'.format(file))
    return False


def evaluate_yes_no_input(message):
    while True:
        user_input = input(message)
        if str(user_input).lower() in ['y', 'n']:
            return user_input.lower()
        else:
            print("Input not recognized. Please enter 'y', or 'n'")


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
            path = unquote('/' + path)
        else:
            bucket = tmp.hostname.replace('.s3.amazonaws.com', '')
            path = unquote(tmp.path)
    else:
        raise Exception('Invalid URL passed to deconstruct_s3_url method: {}'.format(url))

    return bucket, path.lstrip('/')


# converts . and .. and ~ in file-paths. (as well as variable names like %HOME%
def convert_to_abs_path(file_name):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(file_name)))


def human_size(bytes, units=[' bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']):
    """ Returns a human readable string representation of bytes """
    return str(round(bytes, 2)) + units[0] if bytes < 1024 else human_size(bytes / 1024, units[1:])


def is_json(test):
    try:
        json.dumps(test)
        return True
    except:
        return False


def _send_prepared_request(req, timeout=150, deserialize_handler=DeserializeHandler.convert_json,
                           error_handler=HttpErrorHandlingStrategy.print_and_exit, reauth_func=None):
    with requests.Session() as session:
        retries = Retry(total=10,
                        backoff_factor=0.1,
                        status_forcelist=[502, 503, 504])
        prepped = req.prepare()
        logger.debug('{} {} @ {}'.format(prepped.method, prepped.url, datetime.datetime.now()))
        session.mount(prepped.url, HTTPAdapter(max_retries=retries))
        tmp = session.send(prepped, timeout=timeout)

        if tmp.status_code == 401 and reauth_func is not None:
            token_version = getattr(prepped, REQUEST_TOKEN_VERSION_ATTR, None)
            if token_version is None:
                reauth_func()
            else:
                reauth_func(token_version=token_version)
            prepped = req.prepare()
            tmp = session.send(prepped, timeout=timeout)
        logger.debug(
            '{} {} (elapsed = {})- STATUS {}'.format(prepped.method, prepped.url, tmp.elapsed, tmp.status_code))
        if not tmp.ok:
            error_handler(tmp)
    return deserialize_handler(tmp)


def get_request(url, headers={}, auth=None, timeout=150, deserialize_handler=DeserializeHandler.convert_json,
                error_handler=HttpErrorHandlingStrategy.print_and_exit, reauth_func=None):
    req = requests.Request('GET', url, auth=auth, headers=headers)
    return _send_prepared_request(req, timeout=timeout, deserialize_handler=deserialize_handler,
                                  error_handler=error_handler, reauth_func=reauth_func)


def post_request(url, payload=None, headers={}, auth=None, timeout=150,
                 deserialize_handler=DeserializeHandler.convert_json,
                 error_handler=HttpErrorHandlingStrategy.print_and_exit, reauth_func=None):
    data_param, headers = get_data_and_header_params(payload, headers)
    req = requests.Request('POST', url, auth=auth, headers=headers, **data_param)
    return _send_prepared_request(req, timeout=timeout, deserialize_handler=deserialize_handler,
                                  error_handler=error_handler, reauth_func=reauth_func)


def put_request(url, payload=None, headers={}, auth=None, timeout=150,
                deserialize_handler=DeserializeHandler.convert_json,
                error_handler=HttpErrorHandlingStrategy.print_and_exit, reauth_func=None):
    data_param, headers = get_data_and_header_params(payload, headers)
    req = requests.Request('PUT', url, auth=auth, headers=headers, **data_param)
    return _send_prepared_request(req, timeout=timeout, deserialize_handler=deserialize_handler,
                                  error_handler=error_handler, reauth_func=reauth_func)


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


def collect_directory_list():
    while True:
        retry = input(
            '\nPlease specify the immediate parent directory of the associated file paths specified in the csv, then press Enter. Separate multiple directories by a comma: ')
        response = retry.split(',')
        directories = list(map(lambda x: Path(x.strip()), response))
        if all(map(lambda x: os.path.isdir(x), directories)):
            return list(map(lambda x: x.resolve(), directories))
        else:
            not_existent = list(filter(lambda x: not os.path.isdir(x), directories))
            logger.error("The following directories cannot be found:\n")
            for directory in not_existent:
                logger.error(f"\t{directory.resolve()}")


def get_non_blank_input(prompt, input_name):
    while True:
        user_input = input(prompt).strip()
        if user_input:
            return user_input
        else:
            print('{} cannot be blank. Please try again'.format(input_name))


def get_int_input(prompt, input_name):
    while True:
        user_input = get_non_blank_input(prompt, input_name)
        try:
            return int(user_input)
        except ValueError:
            print('{} must be an integer. Please try again'.format(input_name))


def get_directory_input(prompt):
    while True:
        retry_associated_files_dir = Path(input(prompt))
        if not retry_associated_files_dir.exists():
            logger.error(f'{retry_associated_files_dir} does not exist. Please try again.')
        else:
            return retry_associated_files_dir


def tqdm_thread_map(func: Callable, args: List[Tuple], max_workers: int, disable_tqdm: bool = False):
    return thread_map(func, args, max_workers=max_workers, total=len(args), disable=disable_tqdm)


class SqlUtils:
    # ------------------------------------------------------------
    # CREATE SQLITE DATABASE AND TABLE FROM PYDANTIC MODEL
    # ------------------------------------------------------------
    @staticmethod
    def create_table_from_model(conn, model: Type[BaseModel], table_name: str):
        """Creates a table from a Pydantic model."""
        cursor = conn.cursor()

        # Map Python/Pydantic types to SQLite types
        type_map = {
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            bool: "INTEGER",  # SQLite has no BOOL; use INTEGER(0/1)
        }

        columns = []
        for field_name, field in model.model_fields.items():
            python_type = field.annotation
            sql_type = type_map.get(python_type, "TEXT")  # fallback to TEXT
            col_def = f"{field_name} {sql_type}"
            if field_name == "id":
                col_def += " PRIMARY KEY"
            columns.append(col_def)

        column_sql = ", ".join(columns)
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_sql});"

        cursor.execute(sql)
        conn.commit()

    # ------------------------------------------------------------
    # BULK INSERT FROM A LIST OF PYDANTIC MODELS
    # ------------------------------------------------------------
    @staticmethod
    def bulk_insert(conn, table_name: str, objects: List[BaseModel]):
        cursor = conn.cursor()
        if not objects:
            return

        fields = objects[0].model_dump().keys()
        placeholders = ", ".join(["?"] * len(fields))
        sql = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders});"

        values = [tuple(obj.model_dump().values()) for obj in objects]

        cursor.executemany(sql, values)
        conn.commit()

    # ------------------------------------------------------------
    # PAGED QUERY
    # ------------------------------------------------------------
    @staticmethod
    def paged_query(conn, table_name: str, page: int, page_size: int, model: Type[BaseModel],
                    order_by_clause: str = "ID", where_clause: str = "true"):
        assert page > 0, "Page number must be greater than 0"
        cursor = conn.cursor()

        offset = (page - 1) * page_size
        sql = f"""
            SELECT * 
            FROM {table_name}
            WHERE {where_clause}
            ORDER BY {order_by_clause}
            LIMIT ? OFFSET ?;
        """
        cursor.execute(sql, (page_size, offset))

        rows = cursor.fetchall()
        # Convert rows to Pydantic objects
        return [model(**dict(zip([col[0] for col in cursor.description], row)))
                for row in rows]

    @staticmethod
    def does_table_exists(db_connection, param):
        cursor = db_connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (param,))
        return cursor.fetchone() is not None

    @staticmethod
    def query(db_connection, table_name, select_clause="*", where_clause=None, order_by_clause=None, limit=None,
              offset=None):
        cursor = db_connection.cursor()
        sql = f"SELECT {select_clause} FROM {table_name}"
        if where_clause:
            sql += f" WHERE {where_clause} "
        if order_by_clause:
            sql += f" ORDER BY {order_by_clause}"
        if limit:
            sql += f" LIMIT {str(limit)}"
        if offset:
            sql += f" OFFSET {str(offset)}"
        cursor.execute(sql, ())
        return cursor.fetchall()

    @staticmethod
    def update(db_connection, table, set_clause, where_clause):
        cursor = db_connection.cursor()
        sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause};"
        cursor.execute(sql, ())
        cursor.connection.commit()

    @staticmethod
    def run_ddl(db_connection, ddl):
        cursor = db_connection.cursor()
        cursor.execute(ddl)
        cursor.connection.commit()
