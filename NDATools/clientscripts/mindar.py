import argparse
from NDATools.MindarManager import *


def parse_args():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=default)
    parser.add_argument('--url', dest='url')

    subparsers = parser.add_subparsers(dest='subparser_name')

    make_subcommand(subparsers, 'create', create_mindar, provide_create_mindar_arguments)  # mindar create
    make_subcommand(subparsers, 'delete', delete_mindar)  # mindar delete
    make_subcommand(subparsers, 'validate', validate_mindar)  # mindar validate
    make_subcommand(subparsers, 'describe', describe_mindar)  # mindar describe
    make_subcommand(subparsers, 'submit', submit_mindar)  # mindar submit
    make_subcommand(subparsers, 'show', show_mindar)  # mindar show
    make_subcommand(subparsers, 'export', export_mindar)  # mindar export
    make_subcommand(subparsers, 'import', import_mindar)  # mindar import

    table_parser = make_subcommand(subparsers, 'table', default)  # mindar table
    table_subparser = table_parser.add_subparsers(dest='table_subparser_name')
    make_subcommand(table_subparser, 'add', add_table)  # mindar table add
    make_subcommand(table_subparser, 'drop', drop_table)  # mindar table drop
    make_subcommand(table_subparser, 'reset', reset_table)  # mindar table reset

    return parser.parse_args()


def make_subcommand(subparser, command, method, provider=None):
    result = subparser.add_parser(command)
    result.set_defaults(func=method)

    if provider:
        provider(result)

    if provider is not provide_credentials_arguments:
        provide_credentials_arguments(result)

    return result


def provide_create_mindar_arguments(parser):
    parser.add_argument('--package', dest='package', help='Package ID to create miNDAR with')
    parser.add_argument('--nickname', dest='nickname', help='Created miNDAR nickname')


def provide_credentials_arguments(parser):
    parser.add_argument('--username', dest='username', help='NDA username')
    parser.add_argument('--password', dest='password', help='NDA password')
    parser.add_argument('--mpassword', dest='mindar_password', help='miNDAR password')
    parser.add_argument('--mcreds', dest='mindar_cred_file', help='miNDAR credentials file')


def default(args, config, mindar):
    print('Hello, World!')


def create_mindar(args, config, mindar):
    requires_mindar_password(args, True)

    mindar.create_mindar(package_id=args.package, password=args.mindar_password, nickname=args.nickname)


def delete_mindar(args, config, mindar):
    print('Delete, Mindar!')


def describe_mindar(args, config, mindar):
    print('Describe, Mindar!')


def validate_mindar(args, config, mindar):
    print('Validate, Mindar!')


def submit_mindar(args, config, mindar):
    print('Submit, Mindar!')


def show_mindar(args, config, mindar):
    print('Show, Mindar!')


def export_mindar(args, config, mindar):
    print('Export, Mindar!')


def import_mindar(args, config, mindar):
    print('Import, Mindar!')


def add_table(args, config, mindar):
    print('Add, Table!')


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
    if os.path.isfile(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg')):
        config = ClientConfiguration(os.path.join(os.path.expanduser('~'), '.NDATools/settings.cfg'), args.username, args.password, None, None)
    else:
        config = ClientConfiguration('clientscripts/config/settings.cfg', args.username, args.password, None, None)

        config.read_user_credentials()
        config.make_config()

    if args.url:
        config.mindar = args.url

    return config


def main():
    args = parse_args()

    config = load_config(args)
    mindar = MindarManager(config)
    args.func(args, config, mindar)  # execute selected argument function


if __name__ == '__main__':
    main()
