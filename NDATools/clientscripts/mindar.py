import argparse
import concurrent
from concurrent.futures._base import ALL_COMPLETED
from concurrent.futures.thread import ThreadPoolExecutor
import tempfile

from NDATools.clientscripts.vtcmd import validate_files
from NDATools.MindarManager import *
import csv
import time
import atexit
import sys


def parse_args():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=default)
    parser.add_argument('--url', dest='url')
    parser.add_argument('--profile', action='store_true', help='Enable runtime profiling.')

    subparsers = parser.add_subparsers(dest='subparser_name')

    make_subcommand(subparsers, 'create', create_mindar, [create_mindar_args, mindar_password_args])  # mindar create
    make_subcommand(subparsers, 'delete', delete_mindar, [delete_mindar_args, require_schema])  # mindar delete
    make_subcommand(subparsers, 'show', show_mindar, [show_mindar_args])  # mindar show
    make_subcommand(subparsers, 'describe', describe_mindar, [describe_mindar_args, require_schema])  # mindar describe
    make_subcommand(subparsers, 'validate', validate_mindar, [require_schema, validate_mindar_args])  # mindar validate
    make_subcommand(subparsers, 'submit', submit_mindar)  # mindar submit
    make_subcommand(subparsers, 'import', import_mindar, [require_schema, mindar_import_args])  # mindar import
    make_subcommand(subparsers, 'export', export_mindar, [export_mindar_args, require_schema])  # mindar export

    table_parser = make_subcommand(subparsers, 'tables', default)  # mindar table
    table_subparser = table_parser.add_subparsers(dest='table_subparser_name')
    make_subcommand(table_subparser, 'add', add_table, [add_table_args])  # mindar table add
    make_subcommand(table_subparser, 'drop', drop_table, [drop_table_args])  # mindar table drop
    make_subcommand(table_subparser, 'reset', reset_table, [reset_table_args])  # mindar table reset

    return parser.parse_args()


def make_subcommand(subparser, command, method, provider=None):
    result = subparser.add_parser(command)
    result.set_defaults(func=method)

    if isinstance(provider, list):
        for func in provider:
            func(result)
    elif provider:
        provider(result)

    result.add_argument('--username', dest='username', help='NDA username')
    result.add_argument('--password', dest='password', help='NDA password')

    return result


def show_mindar_args(parser):
    parser.add_argument('--include-deleted', dest='include_deleted', action='store_true', help='Include deleted miNDARs in output')


def create_mindar_args(parser):
    # parser.add_argument('--package', dest='package', help='Create mindar using a pre-existing package')
    parser.add_argument('--nickname', dest='nickname', help='Created miNDAR nickname')


def add_table_args(parser):
    parser.add_argument('tables')
    parser.add_argument('--schema', help='Schema to add tables to')


def drop_table_args(parser):
    parser.add_argument('tables')
    parser.add_argument('--schema', help='Schema to drop tables from')


def reset_table_args(parser):
    parser.add_argument('tables')
    parser.add_argument('--schema', help='Schema with affected tables')
    parser.add_argument('-f', '--force', dest='force_delete', action='store_true')


def delete_mindar_args(parser):
    parser.add_argument('-f', '--force', dest='force_delete', action='store_true')


def describe_mindar_args(parser):
    parser.add_argument('--refresh-stats', dest='refresh_stats', action='store_true')


def export_mindar_args(parser):
    parser.add_argument('--tables')
    parser.add_argument('--include-id', action='store_true')
    parser.add_argument('--validate', action='store_true')
    parser.add_argument('--add-nda-header', action='store_true')
    parser.add_argument('--download-dir', help='target directory for download')
    parser.add_argument('--worker-threads', default='1',
                        help='specifies the number of threads to use for exporting csv''s', type=int)


def validate_mindar_args(parser):
    parser.add_argument('--files')
    parser.add_argument('--tables')
    parser.add_argument('--worker-threads', default='1',
                        help='specifies the number of threads to use for exporting/validating csv''s', type=int)
    parser.add_argument('-w', '--warning', action='store_true', help='Returns validation warnings for list of files')
    parser.add_argument('--download-dir', default= 'If no value is specified, exported mindar tables are downloaded into the '
                               'home directory of the user, in a new folder with the same name as the mindar schema',
                        help='directory to store validation results')


def require_schema(parser):
    parser.add_argument('schema', help='miNDAR schema name')


def mindar_password_args(parser):
    parser.add_argument('--mpassword', dest='mindar_password', help='miNDAR password')
    parser.add_argument('--mcreds', dest='mindar_cred_file', help='miNDAR credentials file')


def mindar_import_args(parser):
    parser.add_argument('table', help='miNDAR schema table name')
    parser.add_argument('files', nargs='+', help='CSV data files')
    parser.add_argument('--validate', dest='validate', action='store_true')
    parser.add_argument('--warning', '-w', dest='warning', action='store_true')
    parser.add_argument('--worker-threads', dest='worker_threads', help='How many threads to use for validation')
    parser.add_argument('--continue-on-error', dest='error_continue', action='store_true')
    parser.add_argument('--chunk-size', type=int, default=1000, dest='chunks', help='How many rows should each request contain')


def default(args, config, mindar):
    print('Hello, World!')


def create_mindar(args, config, mindar):
    requires_mindar_password(args, True)

    print('Creating an empty miNDAR, this might take some time...')
    response = mindar.create_mindar(password=args.mindar_password, nickname=args.nickname)
    print()
    print('------ MiNDAR Created ------')
    print("Current Status: {}".format(response['status']))
    print("Package ID: {}".format(response['package_id']))
    print("Package Name: {}".format(response['name']))
    print()
    print("MiNDAR Host Name: {}".format(response['host']))
    print("MiNDAR Port: {}".format(response['port']))
    print("MiNDAR Service: {}".format(response['service']))
    print("MiNDAR Username: {}".format(response['schema']))
    print()
    print("To connect to your miNDAR, download a client like SQL Developer and enter the connection details above."
          " Be sure to enter the password that you specified here")


def delete_mindar(args, config, mindar):
    response = mindar.show_mindars(True)
    match = [r for r in response if r['schema']==args.schema.lower()]
    if not match:
        print('miNDAR {} was not found. Please check your arguments and/or credentials and try again'.format(args.schema))
        return
    elif match[0]['status'] in {'miNDAR Deleted','miNDAR Delete In Progress'}:
        print("miNDAR {} already has a status of '{}' and cannot be deleted at this time.".format(args.schema, match[0]['status']))
        return

    print('Before deleting your miNDAR, please make sure there are no active connections or the delete operation will not succeed.'.format(args.schema))

    if not args.force_delete:
        verify = input('Are you sure you want to delete miNDAR: {}? (Y/N) '.format(args.schema))

        if verify.lower() != 'y':
            print('Aborting.')
            return

    print('Deleting miNDAR: {}'.format(args.schema))

    mindar.delete_mindar(args.schema)

    print('Delete Initiated for miNDAR {}'.format(args.schema))


def validate_mindar(args, config, mindar):
    if args.tables or args.schema:
        if not args.tables and args.schema:
            raise Exception('Schema and table args must both be specified. Missing {} arg'.format(
                'tables' if not args.tables else 'schema'))
        elif args.files:
            raise Exception('Schema/table arguments are incompatible with --files argument.')
    if args.download_dir and args.files:
        print('Warning: download-dir argument was provided, but does not have any affect when used with --files arg')

    if args.files:
        file_list = args.files.split(',')
        invalid_files = []
        for file in file_list:
            if not os.path.isfile(file):
                invalid_files.append(file)
        if invalid_files:
            raise Exception('The following files were not found: {}'.format(','.join(invalid_files)))
    else:
        download_dir = args.download_dir or os.path.normpath('{}/{}'.format(os.path.expanduser('~'), args.schema))
        if not args.tables:
            response = mindar.show_tables(args.schema)
            tables = [ds['shortName'].lower() for ds in response['dataStructures']]
            tables.sort()
        else:
            tables = list(map(lambda x: x.lower(), args.tables.split(',')))

        verify_directory(download_dir)

        file_list = export_mindar_helper(mindar, tables, args.schema, download_dir, False, args.worker_threads, True)
        print('Export of {}/{} tables in schema {} finished at {}'.format(len(file_list), len(tables), args.schema,
                                                                          datetime.now()))
        successful_table_exports = set(map(lambda f: os.path.basename(f).replace('.csv',''), file_list))
        for short_name in tables:
            if short_name not in successful_table_exports:
                print('WARN - validation for table {} will be skipped because it was not successfully '
                      'exported from the mindar'.format(short_name))

    if not file_list:
        print('No valid files exist to validate. Fix arguments and re-run')
        exit_client(signal.SIGTERM)

    validate_files(file_list=file_list, warnings=args.warning, build_package=False, threads=args.worker_threads, config=config)


def submit_mindar(args, config, mindar):
    print('Submit, Mindar!')


def show_mindar(args, config, mindar):
    response = mindar.show_mindars(args.include_deleted)
    num_mindar = len(response)

    if num_mindar <= 0:
        print('This user has no miNDARs, you can create one by executing \'mindar create\'.')
        return

    print('Showing {} miNDARs...'.format(num_mindar))
    print()
    table_format = '{:<40} {:<40} {:<15} {:<25} {:<8}'
    print(table_format.format('Name', 'Schema', 'Package Id', 'Status', 'Created Date'))

    for mindar in response:
        print(table_format.format(mindar['name'],
                                  mindar['schema'],
                                  mindar['package_id'],
                                  mindar['status'],
                                  mindar['created_date']))


def export_mindar(args, config, mindar):
    if args.tables:
        tables = list(map(lambda x: x.lower(), args.tables.split(',')))
    else:
        response = mindar.show_tables(args.schema)
        tables = [ds['shortName'].lower() for ds in response['dataStructures']]
        tables.sort()

    if args.validate and not args.add_nda_header:
        print('WARNING - Adding nda-header to exported files even though --add-nda-header argument was not specified, because it is required for validation')
        args.add_nda_header = True

    download_dir = args.download_dir or os.path.normpath('{}/{}'.format(os.path.expanduser('~'), args.schema))

    verify_directory(download_dir)

    files = export_mindar_helper(mindar, tables, args.schema, download_dir, args.include_id, args.worker_threads, args.add_nda_header)
    print('Export of {}/{} tables in schema {} finished at {}'.format(len(files), len(tables), args.schema, datetime.now()))
    if args.validate:
        validate_files(file_list=files, warnings=False, build_package=False, threads=args.worker_threads, config=config)

    exit_client(signal.SIGTERM)


def export_mindar_helper(mindar, tables, schema, download_dir, include_id=False, worker_threads=1, add_nda_header=False):
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


def import_mindar(args, config, mindar):
    print('Beginning miNDAR import procedure...')
    data = []
    count = 1

    if args.validate:
        validation_files = []

    for file in args.files:
        file_data = []
        header_line = None
        file_name = os.path.basename(file)
        temp = []

        if args.validate:
            split = re.split(r'(\d+)', args.table)
            table_name = split[0]
            table_ver = split[1]
            temp_dir = os.path.realpath(tempfile.mkdtemp())

        print('Chunking: {}...'.format(file_name))

        with open(file, 'r') as f:
            if args.validate:
                temp_path = os.path.join(temp_dir, os.path.basename(file))
                validation_files.append(temp_path)

                if os.path.exists(temp_path):
                    os.remove(temp_path)  # There's a chance that this exact file was written to in temp already

                temp_file = open(temp_path, 'a+')
                temp_file.seek(0)  # sanity check
                temp_file.write('{},{}\n'.format(table_name, table_ver))

            for rows in csv.reader(f, dialect='excel-tab'):
                if args.validate:
                    temp_file.write('{}\n'.format(','.join(rows)))

                temp.append(rows)

                if count % args.chunks == 0:
                    file_data.append(temp)
                    temp = []

                    if header_line:
                        temp.append(header_line)

                count += 1

                if not header_line:
                    header_line = rows
                    count -= 1  # Here to prevent header row from counting as a row towards chunking

            if temp:
                file_data.append(temp)

        data.append(file_data)

        if args.validate:
            temp_file.close()

        print('Finished chunking: {}'.format(file_name))

    file_num = 1

    for file_data in data:
        errored = []
        file_name = os.path.basename(args.files[file_num - 1])

        print('{}:'.format(file_name))
        print('    Total Chunks: {}'.format(len(file_data)))

        chunk_num = 1

        for chunk in file_data:
            sys.stdout.write('    Pushing Chunk #{}...'.format(chunk_num))

            try:
                payload = ''

                for row in chunk:
                    payload += '{}\n'.format(','.join(row))

                mindar.import_data_csv(args.schema, args.table.lower(), payload)
                print('Done!')
            except Exception as e:
                if chunk_num == 1:
                    index = 0
                else:
                    index = chunk_num * args.chunks

                chunk_length = len(chunk)

                if sys.version_info.major >= 3:
                    err = list(range(index, index + chunk_length))
                else:
                    err = range(index, index + chunk_length)

                errored.append(err)

                if not args.error_continue:
                    raise e
                else:
                    print('Failed!')

            chunk_num += 1

        if errored:
            print('{} import completed with errors!'.format(file_name))
            print('{} chunks produced errors, detailed report below: '.format(len(errored)))

            chunk_num = 1

            for err in errored:
                if err:
                    print('Chunk {} - Impacting Row Numbers: {} - {}'.format(chunk_num, err[0], err[-1]))
                else:
                    print('Error reporting failed to properly estimate impacted row numbers, please report this.')

                chunk_num += 1
        else:
            print('{} import successfully completed!'.format(file_name))

        file_num += 1

    if args.validate:
        validate_files(file_list=validation_files, warnings=args.warning, build_package=False, threads=args.worker_threads, config=config)


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
    except Exception as e:
        # an error message will already be printed out from the Utils. class. Do not print out more info
        return False


def drop_table_helper(schema, table, mindar):
    try:
        print('Deleting table {} from schema {}'.format(table, schema))
        mindar.drop_table(schema, table)
        return True
    except Exception as e:
        # an error message will already be printed out from the Utils. class. Do not print out more info
        return False


def add_table(args, config, mindar):
    table_list = list(map(lambda x: x.lower(), args.tables.split(',')))
    # check first that each table doesn't already exist in the mindar
    existing_tables = verify_no_tables_exist(args.schema, table_list, mindar) or set()

    tables = set(table_list) - set(existing_tables)
    if not tables:
        print('Invalid table list specified as a command line argument. Correct argument list and try again.')
        exit_client(signal.SIGTERM)

    success_count = 0
    for table in tables:
        success_count += 1 if add_table_helper(args.schema, table, mindar) else 0

    print()
    print('Finished - {}/{} tables successfully added to schema {}.'.format(success_count, len(tables), args.schema))
    exit_client(signal.SIGTERM)


def drop_table(args, config, mindar):
    table_list = list(map(lambda x: x.lower(), args.tables.split(',')))
    missing_tables = verify_all_tables_exist(args.schema, table_list, mindar) or set()

    tables = set(table_list) - set(missing_tables)
    if not tables:
        print('Invalid table list specified as a command line argument. Correct argument list and try again.')
        exit_client(signal.SIGTERM)

    success_count = 0
    for table in tables:
        success_count += 1 if drop_table_helper(args.schema, table, mindar) else 0

    print()
    print('Finished - {}/{} tables successfully dropped from schema {}'.format(success_count, len(tables), args.schema))
    exit_client(signal.SIGTERM)


def reset_table(args, config, mindar):
    table_list = list(map(lambda x: x.lower(), args.tables.split(',')))

    existing_tables = filter_existing_tables(args.schema, table_list, mindar)

    if not args.force_delete and existing_tables:
        verify = input('If you continue, the data in the following tables will be deleted: {}.'
                       ' Are you sure you want to continue? (Y/N) '.format(','.join(existing_tables)))

        if verify.lower() != 'y':
            print('Aborting.')
            return

    success_count = 0
    for table in table_list:
        if table in existing_tables:
            success = drop_table_helper(args.schema, table, mindar)
            if not success:
                print('skipping add of table {}'.format(table))
                continue
        success_count += 1 if add_table_helper(args.schema, table, mindar) else 0

    print()
    print('Finished - {}/{} tables successfully recreated in schema {}'.format(success_count, len(table_list), args.schema))
    exit_client(signal.SIGTERM)


def describe_mindar(args, config, mindar):
    if args.refresh_stats:
        print('Refreshing stats - this can take several minutes...')
        mindar.refresh_stats(args.schema)
        print('Stats for miNDAR {} have been refreshed'.format(args.schema))

    response = mindar.show_tables(args.schema)
    structures = response['dataStructures']

    if len(structures) <= 0:
        print('This miNDAR has no tables yet. You can add one by executing \'mindar add-table <table-name>\'.')
        return

    structures.sort(key=lambda x: x['shortName'])
    print('Showing {} tables from {}...'.format(len(structures), args.schema))
    print()
    table_format = '{:<35} {:<20}'
    print(table_format.format('Name', 'Approximate Row Count'))

    for table in structures:
        print(table_format.format(table['shortName'], table['rowCount']))

    print()
    print('Note - the row numbers are approximate and based on the most recent statistics that Oracle has gathered for the table''s in your schema.')
    print('To get the most accurate numers, use the --refresh-stats flag. For more information see https://docs.oracle.com/cd/A84870_01/doc/server.816/a76992/stats.htm.')


def request_warning():
    print('Executing request, this might take some time...')


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


def load_config(args):
    config_mutated = False

    if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
        config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'), args.username, args.password, None, None)
    else:
        config = ClientConfiguration(os.path.normpath('clientscripts/config/settings.cfg'), args.username, args.password, None, None)
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


def verify_directory(directory):
    if not os.path.exists(directory):
        os.mkdir(directory)

    if not os.path.isdir(directory):
        print('{} is not a directory!'.format(directory))
        exit_client(signal.SIGTERM)


def print_time_exit(start_time):
    print('Execution took %.2fs' % (time.time() - start_time))


def main():
    args = parse_args()

    if args.profile:
        atexit.register(print_time_exit, start_time=time.time())

    config = load_config(args)
    mindar = MindarManager(config)
    args.func(args, config, mindar)  # execute selected argument function


if __name__ == '__main__':
    main()
