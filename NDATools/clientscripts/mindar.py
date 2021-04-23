import argparse
from NDATools.MindarManager import *
import csv


def parse_args():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=default)
    parser.add_argument('--url', dest='url')

    subparsers = parser.add_subparsers(dest='subparser_name')

    make_subcommand(subparsers, 'create', create_mindar, [create_mindar_args, mindar_password_args])  # mindar create
    make_subcommand(subparsers, 'delete', delete_mindar, [delete_mindar_args, require_schema])  # mindar delete
    make_subcommand(subparsers, 'validate', validate_mindar)  # mindar validate
    make_subcommand(subparsers, 'describe', describe_mindar)  # mindar describe
    make_subcommand(subparsers, 'submit', submit_mindar)  # mindar submit
    make_subcommand(subparsers, 'show', show_mindar)  # mindar show
    make_subcommand(subparsers, 'export', export_mindar)  # mindar export
    make_subcommand(subparsers, 'import', import_mindar)  # mindar import

    table_parser = make_subcommand(subparsers, 'table', default)  # mindar table
    table_subparser = table_parser.add_subparsers(dest='table_subparser_name')
    make_subcommand(table_subparser, 'add', add_table, [require_schema, add_table_args])  # mindar table add
    make_subcommand(table_subparser, 'show', show_table, require_schema)  # mindar table show
    make_subcommand(table_subparser, 'drop', drop_table)  # mindar table drop
    make_subcommand(table_subparser, 'reset', reset_table)  # mindar table reset

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


def create_mindar_args(parser):
    parser.add_argument('--package', dest='package', help='Package ID to create miNDAR with')
    parser.add_argument('--nickname', dest='nickname', help='Created miNDAR nickname')


def add_table_args(parser):
    parser.add_argument('tables')


def delete_mindar_args(parser):
    parser.add_argument('-f', '--force', dest='force_delete', action='store_true')


def require_schema(parser):
    parser.add_argument('schema')


def mindar_password_args(parser):
    parser.add_argument('--mpassword', dest='mindar_password', help='miNDAR password')
    parser.add_argument('--mcreds', dest='mindar_cred_file', help='miNDAR credentials file')


def default(args, config, mindar):
    print('Hello, World!')


def create_mindar(args, config, mindar):
    requires_mindar_password(args, True)

    if args.package:
        print(f'Creating a mindar for package {args.package}')
    else:
        print('Creating an empty mindar...')

    print('Executing request, this might take some time...')
    response = mindar.create_mindar(package_id=args.package, password=args.mindar_password, nickname=args.nickname)

    print()
    print('------ Mindar Creation Initiated ------')
    print(f"Mindar ID: {response['mindar_id']}")
    print(f"Package ID: {response['package_id']}")
    print(f"Package Name: {response['name']}")
    print(f"Mindar Schema: {response['schema']}")
    print(f"Current Status: {response['status']}")
    print("--- Connection Info")
    print(f"Host: {response['host']}")
    print(f"Port: {response['port']}")
    print(f"Service Name: {response['service']}")
    print(f"Username: {response['schema']}")
    print()
    print('You may not be able to connect to your mindar until the creation process is complete!')


def delete_mindar(args, config, mindar):
    if not args.force_delete:
        verify = input('Are you sure you want to delete mindar: {}? (Y/N) '.format(args.schema))

        if verify.lower() != 'y':
            print('Aborting.')
            return

    print(f'Deleting mindar: {args.schema}')

    print('Executing request, this might take some time...')
    response = mindar.delete_mindar(args.schema)

    print(response['message'])


def describe_mindar(args, config, mindar):
    print('Describe, Mindar!')


def validate_mindar(args, config, mindar):
    print('Validate, Mindar!')


def submit_mindar(args, config, mindar):
    print('Submit, Mindar!')


def show_mindar(args, config, mindar):
    print('Executing request, this might take some time...')
    response = mindar.show_mindars()
    num_mindar = len(response)

    if num_mindar <= 0:
        print('This user has no mindars, you can create one by executing \'mindar create\'.')
        return

    print(f'Showing {num_mindar} mindars...')
    print()
    print('Name,Schema,Mindar Id,Package Id,Status,Created Date')

    for mindar in response:
        print("{},{},{},{},{},{}".format(mindar['name'],
                                         mindar['schema'],
                                         mindar['mindar_id'],
                                         mindar['package_id'],
                                         mindar['status'],
                                         mindar['created_date']))


def export_mindar(args, config, mindar):
    print('Export, Mindar!')


def import_mindar(args, config, mindar):
    print('Import, Mindar!')


def add_table(args, config, mindar):
    table_list = args.tables.split(',')

    for table in table_list:
        print('Adding table {} to schema {}'.format(table, args.schema))
        response = mindar.add_table(args.schema, table)
        print(response)


def show_table(args, config, mindar):
    print('Show, Table!')


def drop_table(args, config, mindar):
    print('Drop, Table!')


def reset_table(args, config, mindar):
    print('Reset, Table!')


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
