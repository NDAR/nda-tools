import argparse
from NDATools.MindarManager import *
from datetime import datetime

def parse_args():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=default)
    parser.add_argument('--url', dest='url')

    subparsers = parser.add_subparsers(dest='subparser_name')

    make_subcommand(subparsers, 'create', create_mindar, [create_mindar_args, mindar_password_args])  # mindar create
    make_subcommand(subparsers, 'delete', delete_mindar, [delete_mindar_args, require_schema])  # mindar delete
    make_subcommand(subparsers, 'show', show_mindar, [show_mindar_args])  # mindar show
    make_subcommand(subparsers, 'describe', describe_mindar, [describe_mindar_args, require_schema])  # mindar describe
    make_subcommand(subparsers, 'validate', validate_mindar)  # mindar validate
    make_subcommand(subparsers, 'submit', submit_mindar)  # mindar submit
    make_subcommand(subparsers, 'export', export_mindar, [export_mindar_args, require_schema])  # mindar export
    make_subcommand(subparsers, 'import', import_mindar)  # mindar import

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
    parser.add_argument('--include-deleted', action='store_true', help='Include deleted miNDARs in output')


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


def delete_mindar_args(parser):
    parser.add_argument('-f', '--force', dest='force_delete', action='store_true')


def describe_mindar_args(parser):
    parser.add_argument('--refresh-stats', dest='refresh_stats', action='store_true')


def export_mindar_args(parser):
    parser.add_argument('--tables')
    parser.add_argument('--include-id', action='store_true')
    parser.add_argument('--download-dir', help='target directory for download')


def require_schema(parser):
    parser.add_argument('schema')


def mindar_password_args(parser):
    parser.add_argument('--mpassword', dest='mindar_password', help='miNDAR password')
    parser.add_argument('--mcreds', dest='mindar_cred_file', help='miNDAR credentials file')


def default(args, config, mindar):
    print('Hello, World!')


def create_mindar(args, config, mindar):
    requires_mindar_password(args, True)

    print('Creating an empty mindar, this might take some time...')
    response = mindar.create_mindar(password=args.mindar_password, nickname=args.nickname)
    print()
    print('------ Mindar Created ------')
    print("Current Status: {}".format(response['status']))
    print("Package ID: {}".format(response['package_id']))
    print("Package Name: {}".format(response['name']))
    print()
    print("Mindar Host Name: {}".format(response['host']))
    print("Mindar Port: {}".format(response['port']))
    print("Mindar Service: {}".format(response['service']))
    print("Mindar Username: {}".format(response['schema']))
    print()
    print("To connect to your miNDAR, download a client like SQL Developer and enter the connection details above."
          " Be sure to enter the password that you specified here")


def delete_mindar(args, config, mindar):
    print('Before deleting your miNDAR, please make sure there are no active connections or the delete operation will not succeed.'.format(args.schema))

    if not args.force_delete:
        verify = input('Are you sure you want to delete mindar: {}? (Y/N) '.format(args.schema))

        if verify.lower() != 'y':
            print('Aborting.')
            return

    print('Deleting mindar: {}'.format(args.schema))

    response = mindar.delete_mindar(args.schema)

    print('Delete Initiated for miNDAR {}'.format(args.schema))


def validate_mindar(args, config, mindar):
    print('Validate, Mindar!')


def submit_mindar(args, config, mindar):
    print('Submit, Mindar!')


def show_mindar(args, config, mindar):
    response = mindar.show_mindars(args.include_deleted)
    num_mindar = len(response)

    if num_mindar <= 0:
        print('This user has no mindars, you can create one by executing \'mindar create\'.')
        return

    print('Showing {} mindars...'.format(num_mindar))
    print()
    table_format ='{:<40} {:<40} {:<15} {:<25} {:<8}'
    print(table_format.format('Name','Schema','Package Id','Status','Created Date'))

    for mindar in response:
        print(table_format.format(mindar['name'],
                                         mindar['schema'],
                                         mindar['package_id'],
                                         mindar['status'],
                                         mindar['created_date']))


def export_mindar(args, config, mindar):
    if args.tables:
        tables = args.tables.split(',')
    else:
        response = mindar.show_tables(args.schema)
        tables = [ ds['shortName'].lower() for ds in response['dataStructures'] ]
        tables.sort()

    dir = args.download_dir or '{}/{}'.format(os.path.expanduser('~'), args.schema)
    success_count = 0
    for table in tables:
        try:
            mindar.export_table_to_file(args.schema, table, dir, args.include_id)
            success_count += 1
        except Exception as e:
            print('Error while trying to export table {}. Error was {}'.format(table, e))
            # for debugging
            # print (get_stack_trace())
            logging.error(get_stack_trace())

    print()
    print('Export of {}/{} tables in schema {} finished at {}'.format(success_count, len(tables), args.schema, datetime.now()))
    exit_client(signal.SIGTERM)


def import_mindar(args, config, mindar):
    print('Import, Mindar!')


def filter_existing_tables(schema, test_tables, mindar):
    table_list = set(map(lambda x: x.lower(), test_tables))
    response = mindar.show_tables(schema)
    structures = { ds['shortName'].lower() for ds in response['dataStructures'] }
    return list(filter(lambda table: table in structures, table_list))


def verify_all_tables_exist(schema, test_tables, mindar):
    existing_tables = filter_existing_tables(schema, test_tables, mindar)
    missing_tables = set(filter(lambda table: table not in existing_tables, test_tables))
    if missing_tables:
        print('The following structures do not exist in the mindar and cannot be used with the ''drop'' or ''reset'' command at this time: {}'.format(','.join(missing_tables)))
        exit_client(signal.SIGTERM)


def verify_no_tables_exist(schema, test_tables, mindar):
    existing_tables = filter_existing_tables(schema, test_tables, mindar)
    if existing_tables:
        print('The following structures already exist in the mindar and cannot be added at this time: {}'.format(','.join(existing_tables)))
        exit_client(signal.SIGTERM)


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
    table_list = args.tables.split(',')
    # check first that each table doesn't already exist in the mindar
    verify_no_tables_exist(args.schema, table_list, mindar)

    success_count = 0
    for table in table_list:
        success_count += 1 if add_table_helper(args.schema, table, mindar) else 0

    print()
    print('Finished - {}/{} tables successfully added to schema {}.'.format(success_count, len(table_list), args.schema))
    exit_client(signal.SIGTERM)


def drop_table(args, config, mindar):
    table_list = args.tables.split(',')
    verify_all_tables_exist(args.schema, table_list, mindar)

    success_count = 0
    for table in table_list:
        success_count += 1 if drop_table_helper(args.schema, table, mindar) else 0

    print()
    print('Finished - {}/{} tables successfully dropped from schema {}'.format(success_count, len(table_list), args.schema))
    exit_client(signal.SIGTERM)


def reset_table(args, config, mindar):
    table_list = args.tables.split(',')

    existing_tables = filter_existing_tables(args.schema, table_list, mindar)

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
        print('Stats for mindar {} have been refreshed'.format(args.schema))

    response = mindar.show_tables(args.schema)
    structures = response['dataStructures']

    if len(structures) <= 0:
        print('This mindar has no tables yet. You can add one by executing \'mindar add-table <table-name>\'.')
        return

    structures.sort(key=lambda x: x['shortName'])
    print('Showing {} tables from {}...'.format(len(structures), args.schema))
    print()
    table_format ='{:<35} {:<20}'
    print(table_format.format('Name','Approximate Row Count'))

    for table in structures:
        print(table_format.format(table['shortName'], table['rowCount']))

    print()
    print('Note - the row numbers are approximate and based on the most recent statistics that Oracle has gathered for the table''s in your schema.')
    print('To get the most accurate numers, use the --refresh-stats flag. For more information see https://docs.oracle.com/cd/A84870_01/doc/server.816/a76992/stats.htm.')


def request_warning():
    print('Executing request, this might take some time...')


def requires_mindar_password(args, confirm=False):
    if not args.mindar_password and not args.mindar_cred_file:
        args.mindar_password = getpass.getpass('Please enter this mindar\'s access password: ')

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
        print('Loaded mindar password from credentials file!')


def load_config(args):
    config_mutated = False

    if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
        config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'), args.username, args.password, None, None)
    else:
        config = ClientConfiguration('clientscripts/config/settings.cfg', args.username, args.password, None, None)
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


def main():
    args = parse_args()

    config = load_config(args)
    mindar = MindarManager(config)
    args.func(args, config, mindar)  # execute selected argument function


if __name__ == '__main__':
    main()
