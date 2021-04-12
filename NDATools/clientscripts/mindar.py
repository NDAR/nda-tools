import argparse
import getpass


def parse_args():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=default)

    subparsers = parser.add_subparsers(dest='subparser_name')

    make_subcommand(subparsers, 'create', create_mindar)  # mindar create
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

    if provider is not None:
        provider(result)

    if provider is not provide_credentials_arguments:
        provide_credentials_arguments(result)

    return result


def provide_credentials_arguments(parser):
    parser.add_argument('--username', dest='username')
    parser.add_argument('--password', dest='password')
    parser.add_argument('--creds', dest='cred_file')


def default(args):
    print('Hello, World!')


def create_mindar(args):
    print('Create, Mindar!')


def delete_mindar(args):
    print('Delete, Mindar!')


def describe_mindar(args):
    print('Describe, Mindar!')


def validate_mindar(args):
    print('Validate, Mindar!')


def submit_mindar(args):
    print('Submit, Mindar!')


def show_mindar(args):
    print('Show, Mindar!')


def export_mindar(args):
    print('Export, Mindar!')


def import_mindar(args):
    print('Import, Mindar!')


def add_table(args):
    print('Add, Table!')


def drop_table(args):
    print('Drop, Table!')


def reset_table(args):
    print('Reset, Table!')


def are_credentials_present(args):
    return (args.cred_file is not None) or (args.username is not None)


def load_credentials(args):
    global username
    global password

    if args.username is not None:
        username = args.username

        if args.password is not None:
            password = args.password
        else:
            password = getpass.getpass('Enter password: ')
    else:
        print('Stubbed: Load credentials file')


def main():
    args = parse_args()

    if are_credentials_present(args):
        load_credentials(args)
        args.func(args)  # execute selected argument function
    else:
        print('Missing credentials information, requires either --username or --creds')


username = ''
password = ''

if __name__ == '__main__':
    main()
