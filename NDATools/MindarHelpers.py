"""
This class exists to prevent circular dependency errors between the helper functions and MindarSubmission logic
"""

import os
import concurrent
import getpass
import time

from concurrent.futures._base import ALL_COMPLETED
from concurrent.futures.thread import ThreadPoolExecutor
from NDATools.Configuration import ClientConfiguration


__all__ = ['requires_mindar_password', 'get_export_dir', 'export_mindar_helper',
    'verify_no_tables_exist', 'verify_all_tables_exist', 'load_config', 'drop_table_helper', 'add_table_helper',
    'filter_existing_tables', 'print_time_exit']


def export_mindar_helper(mindar, tables, schema, download_dir, include_id=False, worker_threads=1, add_nda_header=False):
    verify_directory(download_dir)

    files = []

    def increment_success(f):
        if not f.exception():
            files.append(f.result())

    with ThreadPoolExecutor(max_workers=worker_threads) as executor:
        tasks = []
        for table in tables:
            t = executor.submit(mindar.export_table_to_file, schema, table, download_dir, include_id, add_nda_header)
            tasks.append(t)
            t.add_done_callback(increment_success)
    concurrent.futures.wait(tasks, timeout=None, return_when=ALL_COMPLETED)
    return files


def filter_existing_tables(schema, test_tables, mindar):
    table_list = set(map(lambda x: x.lower(), test_tables))
    response = mindar.show_tables(schema)
    structures = {ds['shortName'].lower() for ds in response['dataStructures']}
    return list(filter(lambda table: table in structures, table_list))


def verify_all_tables_exist(schema, test_tables, mindar):
    existing_tables = filter_existing_tables(schema, test_tables, mindar)
    missing_tables = set(filter(lambda table: table not in existing_tables, test_tables))
    if missing_tables:
        print('WARNING: The following structures were specified as an argument but do not exist in the mindar: {}'.format(','.join(missing_tables)))
        return missing_tables


def verify_no_tables_exist(schema, test_tables, mindar):
    existing_tables = filter_existing_tables(schema, test_tables, mindar)
    if existing_tables:
        print('WARNING: The following structures were specified as an argument but they already exist in the mindar'
              ' so they will not be processed at this time: {}'.format(','.join(existing_tables)))
        return existing_tables


def add_table_helper(schema, table, mindar):
    try:
        print('Adding table {} to schema {}'.format(table, schema))
        mindar.add_table(schema, table)
        return True
    except Exception:
        # an error message will already be printed out from the Utils. class. Do not print out more info
        return False


def requires_mindar_password(args, confirm=False):
    if not args.mindar_password and not args.mindar_cred_file:
        args.mindar_password = getpass.getpass('Please enter this miNDAR\'s access password: ')

        if confirm:
            confirm_password = ''

            while confirm_password != args.mindar_password:
                confirm_password = getpass.getpass('Please verify your password: ')

                if confirm_password != args.mindar_password:
                    print('Your passwords do not match, please try again.')
    elif args.mindar_cred_file:  # TODO Technical Debt: Use a more standardized format for credentials
        print('Opening credentials file...')
        with open(args.mindar_cred_file, 'r') as cred_file:  # TODO: Verify secure permissions before
            args.mindar_password = cred_file.read()
        print('Loaded miNDAR password from credentials file!')


def get_export_dir(directory, schema):
    return directory or '{}/{}'.format(os.path.expanduser('~'), schema)


def verify_directory(directory):
    if not os.path.exists(directory):
        os.mkdir(directory)

    if not os.path.isdir(directory):
        raise ValueError('{} is not a directory!'.format(directory))


def print_time_exit(start_time):
    print('Execution took %.2fs' % (time.time() - start_time))


def load_config(args):
    config_mutated = False

    ak = sk = None
    if hasattr(args, 'accessKey'):
        ak = args.accessKey
    if hasattr(args, 'secretKey'):
        sk= args.secretKey


    if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
        config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'), args.username, args.password, ak, sk)
    else:
        config = ClientConfiguration('clientscripts/config/settings.cfg', args.username, args.password, ak, sk)
        config_mutated = True

        config.read_user_credentials()

    if args.url:
        config.mindar = args.url
        config_mutated = True

    if not config.password or not config.username:
        print('Missing or malformed credentials in settings.cfg')
        config.read_user_credentials()
        config_mutated = True

    if config_mutated:
        config.make_config()

    return config


def drop_table_helper(schema, table, mindar):
    try:
        print('Deleting table {} from schema {}'.format(table, schema))
        mindar.drop_table(schema, table)
        return True
    except Exception:
        # an error message will already be printed out from the Utils. class. Do not print out more info
        return False
