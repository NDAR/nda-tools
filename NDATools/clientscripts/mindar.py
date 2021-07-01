import argparse
import itertools
import tempfile
import csv
import time
import atexit
import sys
import os
import signal
import re

from datetime import datetime

from NDATools.clientscripts.vtcmd import *
from NDATools.MindarManager import *
from NDATools.MindarSubmission import *
from NDATools.MindarHelpers import *
from NDATools.Submission import *
from NDATools.Utils import advanced_request
from NDATools.Utils import get_stack_trace, exit_client


def parse_args():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=default)

    subparsers = parser.add_subparsers(dest='subparser_name')

    make_subcommand(subparsers, 'create', create_mindar, [create_mindar_args, mindar_password_args])  # mindar create
    make_subcommand(subparsers, 'delete', delete_mindar, [delete_mindar_args, require_schema])  # mindar delete
    make_subcommand(subparsers, 'show', show_mindar, [show_mindar_args])  # mindar show
    make_subcommand(subparsers, 'describe', describe_mindar, [describe_mindar_args, require_schema])  # mindar describe
    make_subcommand(subparsers, 'submit', submit_mindar, [require_schema, submit_mindar_args])  # mindar submit
    make_subcommand(subparsers, 'validate', validate_mindar, [require_schema, validate_mindar_args])  # mindar validate
    make_subcommand(subparsers, 'import', import_mindar, [require_schema, mindar_import_args])  # mindar import
    make_subcommand(subparsers, 'export', export_mindar, [export_mindar_args, require_schema])  # mindar export

    table_parser = make_subcommand(subparsers, 'tables', default)  # mindar table
    table_subparser = table_parser.add_subparsers(dest='table_subparser_name')
    make_subcommand(table_subparser, 'add', add_table, [require_schema, add_table_args])  # mindar table add
    make_subcommand(table_subparser, 'drop', drop_table, [require_schema, drop_table_args])  # mindar table drop
    make_subcommand(table_subparser, 'reset', reset_table, [require_schema, reset_table_args])  # mindar table reset

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
    result.add_argument('--url', dest='url', help='Change what miNDAR api url is being used')
    result.add_argument('--profile', action='store_true', help='Enable runtime profiling.')

    return result


def show_mindar_args(parser):
    parser.add_argument('--include-deleted', dest='include_deleted', action='store_true',
                        help='Include deleted miNDARs in output')


def create_mindar_args(parser):
    # parser.add_argument('--package', dest='package', help='Create mindar using a pre-existing package')
    parser.add_argument('--nickname', dest='nickname',
                        help='Created miNDAR nickname')


def add_table_args(parser):
    parser.add_argument('tables', help='Comma separated list of tables')


def drop_table_args(parser):
    parser.add_argument('tables', help='Comma separated list of tables')


def reset_table_args(parser):
    parser.add_argument('tables', help='Comma separated list of tables')
    parser.add_argument('-f', '--force', dest='force_delete', action='store_true',
                        help='Skips delete confirmation dialogue')


def delete_mindar_args(parser):
    parser.add_argument('-f', '--force', dest='force_delete', action='store_true',
                        help='Skips delete confirmation dialogue')


def describe_mindar_args(parser):
    parser.add_argument('--refresh-stats', dest='refresh_stats', action='store_true',
                        help='Forces a statistics refresh before fetching miNDAR information')


def export_mindar_args(parser):
    parser.add_argument('--tables', help='Comma separated list of tables')
    parser.add_argument('--include-id', action='store_true', help='Will include row-id in the exported files')
    parser.add_argument('--validate', action='store_true', help='Will trigger a validation once export is complete')
    parser.add_argument('--add-nda-header', action='store_true',
                        help='Will append NDA data structure information heading to exported file')
    parser.add_argument('--download-dir', help='target directory for download')
    parser.add_argument('--worker-threads', default='1',
                        help='specifies the number of threads to use for exporting csv''s', type=int)


def validate_mindar_args(parser):
    parser.add_argument('--files',
                        help='Allows designating specific files to validate, rather than exporting mindar data')
    parser.add_argument('--tables', help='Comma separated list of tables')
    parser.add_argument('--worker-threads', default='1',
                        help='specifies the number of threads to use for exporting/validating csv''s', type=int)
    parser.add_argument('-w', '--warning', action='store_true', help='Returns validation warnings for list of files')
    parser.add_argument('--download-dir',
                        help='If no value is specified, exported mindar tables are downloaded into the '
                        'home directory of the user, in a new folder with the same name as the mindar schema')


def submit_mindar_args(parser):
    parser.add_argument('-c', '--collection-id', type=int, dest='collectionID', action='store',
                        help='The NDA collection ID', required=True)
    parser.add_argument('--tables', help='MiNDAR tables, comma separated')
    parser.add_argument('--worker-threads', dest='workerThreads', type=int, default=1,
                        help='specifies the number of threads to use for exporting/validating csv''s')
    parser.add_argument('--download-dir', help='target directory for download')
    parser.add_argument('-l', '--list-dir', dest='listDir', type=str, nargs='+', action='store',
                        help='Specifies the directories in which the associated files are files located.')
    parser.add_argument('-m', '--manifest-path', dest='manifestPath', type=str, nargs='+', action='store',
                        help='Specifies the directories in which the manifest files are located')
    parser.add_argument('-s3', '--s3-bucket', dest='s3Bucket', type=str, action='store',
                        help='Specifies the s3 bucket in which the associated files are files located.')
    parser.add_argument('-pre', '--s3-prefix', dest='s3Prefix', type=str, action='store', default='',
                        help='Specifies the s3 prefix in which the associated files are files located.')
    parser.add_argument('-w', '--warning', action='store_true', help='Returns validation warnings for list of files')
    parser.add_argument('-t', '--title', type=str, action='store', help='The title of the submission')
    parser.add_argument('-s', '--scope', type=str, action='store',
                        help='Flag whether to validate using a custom scope. Must enter a custom scope')
    parser.add_argument('-r', '--resume', action='store_true',
                        help='Restart an in-progress submission, resuming from the last successful part in a multi-part'
                             'upload. Must enter a valid submission ID.')
    parser.add_argument('-bc', '--batch', metavar='<arg>', type=int, action='store', default='10000', help='Batch size')

    parser.add_argument('-ak', '--accessKey', metavar='<arg>', type=str, action='store', help='AWS access key')

    parser.add_argument('-sk', '--secretKey', metavar='<arg>', type=str, action='store', help='AWS secret key')
    parser.add_argument('--skip-s3-file-check', action='store_true',
                        help='Skips the check performed to verify that s3 files exist where expected')
    parser.add_argument('-cn', '--create-new', action='store_true', help='The default behavior of the submit '
                               'command is to create a single submission for each structure in a miNDAR. This flag '
                               'should be specified if the user wants to replace any submissions that have already been created. '
                               'Note - If this option is specified, any in-progress submissions will be abandoned')

def require_schema(parser):
    parser.add_argument('schema', help='MiNDAR schema name')


def mindar_password_args(parser):
    parser.add_argument('--mpassword', dest='mindar_password', help='miNDAR password')


def mindar_import_args(parser):
    parser.add_argument('table', help='miNDAR schema table name')
    parser.add_argument('files', nargs='+', help='CSV data files')
    parser.add_argument('--validate', dest='validate', action='store_true',
                        help='Run validation on imported files after import')
    parser.add_argument('--warning', '-w', dest='warning', action='store_true',
                        help='During validation warnings will be logged')
    parser.add_argument('--worker-threads', dest='worker_threads', help='How many threads to use for validation')
    parser.add_argument('--continue-on-error', dest='error_continue', action='store_true',
                        help='Errors encountered by the server during import will not terminate the import process')
    parser.add_argument('--chunk-size', type=int, default=1000, dest='chunks',
                        help='How many rows should each request contain')


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
    match = [r for r in response if r['schema'] == args.schema.lower()]
    if not match:
        print('miNDAR {} was not found. Please check your arguments and/or credentials and try again'.format(args.schema))
        return
    elif match[0]['status'] in {'miNDAR Deleted', 'miNDAR Delete In Progress'}:
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
    if args.files and args.schema:
        raise Exception('Schema argument are incompatible with --files argument. Please specify one or the other')
    if args.download_dir and args.files:
        print('WARNING: download-dir argument was provided, but does not have any affect when used with --files arg')

    if args.files:
        file_list = args.files.split(',')
        invalid_files = []
        for file in file_list:
            if not os.path.isfile(file):
                invalid_files.append(file)
        if invalid_files:
            raise Exception('The following files were not found: {}'.format(','.join(invalid_files)))
    else:
        download_dir = get_export_dir(args.download_dir, args.schema)
        if not args.tables:
            response = mindar.show_tables(args.schema)
            tables = [ds['shortName'].lower() for ds in response['dataStructures']]
            tables.sort()
        else:
            tables = set(map(lambda x: x.lower(), args.tables.split(',')))

        file_list = export_mindar_helper(mindar, tables, args.schema, download_dir, False, args.worker_threads, True)

        print('Export of {}/{} tables in schema {} finished at {}'.format(len(file_list), len(tables), args.schema,
                                                                          datetime.now()))
        successful_table_exports = set(map(lambda f: os.path.basename(f).replace('.csv', ''), file_list))
        for short_name in tables:
            if short_name not in successful_table_exports:
                print('WARN - validation for table {} will be skipped because it was not successfully '
                      'exported from the mindar'.format(short_name))

    if not file_list:
        print('No valid files exist to validate. Fix arguments and re-run')
        exit_client(signal.SIGTERM)

    validate_files(file_list=file_list, warnings=args.warning, build_package=False, threads=args.worker_threads, config=config)


# The initial version of this tool expects 1 data-structure per submission.
def validate_mindar_submission_state(mindar_submission_data):
    tables_with_submissions = list(filter(lambda x: x['submission_id'] is not None, mindar_submission_data.values()))
    submission_ids = [t[0] for t in itertools.groupby(tables_with_submissions, lambda x: x['submission_id']) if len(list(t[1])) > 1]

    if len(submission_ids) > 0:
        m = 'Detected submissions with multiple tables in this mindar. This version of the NDA-Tools client does ' \
            'not currently support processing mindar submissions in this situation. Please contact NDA Help Desk for assistance'
        raise Exception(m)


def submit_mindar(args, config, mindar):

    # do this here because the vtcmd was written in a way that expects certain properties to be set on config and not args
    # and we will be invoking several methods from the vtcmd script from 'submit_mindar'
    config.update_with_args(args)


    if args.create_new and args.resume:
        print('Incompatible arguments detected! --clear-old is incompatible with --resume!')
        exit_client()

    if args.resume:
        # Eventually we want to remove the --resume flag, but we do not want to break any scripts that abcd might have
        # already made for this submission cycle.
        print('\r\nWARNING - the default behavior in this version of the submit command is to resume existing submissions. '
              'The --resume flag no longer needs to be specified and will be removed in a future release\r\n')

    if config.skip_s3_file_check and not config.source_bucket:
        print('Value for --s3-bucket argument must be provided if --skip-s3-file-check is set. Please correct command line arguments before re-running the command')
        exit_client()

    if args.tables:
        tables = list(map(lambda x: x.lower(), args.tables.split(',')))
    else:
        response = mindar.show_tables(args.schema)
        tables = [ds['shortName'].lower() for ds in response['dataStructures']]
        tables.sort()

    success_count = 0
    print('Checking existing submissions for miNDAR...')
    mindar_table_submission_data = mindar.get_mindar_submissions(args.schema)
    validate_mindar_submission_state(mindar_table_submission_data)

    if not tables:
        print('No valid tables provided.')
        exit_client(signal.SIGTERM)

    complete_statuses = [Status.COMPLETE, Status.SUBMITTED_PROTOTYPE, Status.UPLOAD_COMPLETE]

    # Run submission logic
    for table in tables:
        try:
            mindar_submission = MindarSubmission(args.schema, table, MindarSubmissionStep.INITIATE, mindar)
            if table in mindar_table_submission_data:
                table_submission_data = mindar_table_submission_data[table]
                if args.create_new:
                    # An incomplete submission is present
                    print('Clearing old submission for {}...'.format(table))
                    mindar.clear_mindar_submission(args.schema, table)
                    print('Submission cleared!')
                else:
                    if table_submission_data['validation_uuid']:
                        mindar_submission.validation_uuid = [table_submission_data['validation_uuid']]  # has to be an array
                        mindar_submission.set_step(MindarSubmissionStep.SUBMISSION_PACKAGE)

                    if table_submission_data['submission_package_id']:
                        mindar_submission.package_id = table_submission_data['submission_package_id']
                        mindar_submission.set_step(MindarSubmissionStep.CREATE_SUBMISSION)

                    if table_submission_data['submission_id']:
                        mindar_submission.submission_id = table_submission_data['submission_id']
                        mindar_submission.set_step(MindarSubmissionStep.UPLOAD_ASSOCIATED_FILES)

                    if mindar_submission.submission_id:
                        submission = Submission(id=mindar_submission.submission_id, full_file_path=None,
                                                thread_num=args.workerThreads, submission_config=config, resume=True)
                        submission.check_status()

                        if submission.status in complete_statuses:
                            print("Skipping submission for {}, already completed.".format(table))
                            success_count += 1
                            continue

                # get the associated files
                if mindar_submission.get_step() == MindarSubmissionStep.SUBMISSION_PACKAGE:
                    print('Retrieving associated files...')
                    response = advanced_request(endpoint='{}/{{}}'.format(config.validation_api), path_params=mindar_submission.validation_uuid)
                    mindar_submission.associated_files = [response['associated_file_paths']] # Validation.associated files is a list of lists, for some reason

            submission_id_message = '' if not mindar_submission.submission_id else ' (submission-id: {})'.format(mindar_submission.submission_id)
            print('Beginning submission process for: {}{}...'.format(table, submission_id_message))

            # set default values for dataset title and description
            if not config.title:
                config.title = 'DATA ENCLAVE SUBMISSION {} - TABLE {}'.format(args.schema, table)
            config.description = 'DATA ENCLAVE SUBMISSION {} - TABLE {}'.format(args.schema, table)

            mindar_submission.process(args, config)  # Begin submission process
            success_count += 1
        except Exception as e:
            print(e)
            print(get_stack_trace())
            print('Aborting submission for {} due to error during previous step...'.format(table))

    print('Finished creating submissions for {} out of {} tables in the {} miNDAR'
          .format(success_count, len(tables), args.schema))


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

    download_dir = get_export_dir(args.download_dir, args.schema)

    verify_directory(download_dir)

    files = export_mindar_helper(mindar, tables, args.schema, download_dir, args.include_id, args.worker_threads, args.add_nda_header)

    tables_with_errors = set(tables) - set(map(lambda x: os.path.basename(x).replace('.csv',''), files))
    if tables_with_errors:
        tables_with_errors = list(tables_with_errors)
        tables_with_errors.sort()
        print()
        print('An error occurred during export of the following tables:\n{}'.format('\n'.join(tables_with_errors)))
        print()

    print('Export of {}/{} tables in schema {} finished at {}'.format(len(files), len(tables), args.schema, datetime.now()))

    if args.validate:
        validate_files(file_list=files, warnings=False, build_package=False, threads=args.worker_threads, config=config)

    exit_client(signal.SIGTERM)


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

                # TODO: Scale down the memory usage of this by creating a MindarErroredRows object
                # TODO: this object will store first row and then last row as well as a list of chunks it represents
                if sys.version_info.major >= 3:
                    err = list(range(index, index + chunk_length))
                else:
                    err = range(index, index + chunk_length)

                errored.append((chunk_num, err))

                if not args.error_continue:
                    raise e
                else:
                    print('Failed!')

            chunk_num += 1

        # TODO: write some algo to process the errored data and to group the subsequent #s after eachother,
        # TODO: e.g. Chunk 1 0-100, Chunk 2 101-200, Chunk 3 201-300 -> 0-300 (Chunks 1-3)
        if errored:
            print('{} import completed with errors!'.format(file_name))
            print('{} chunks produced errors, detailed report below: '.format(len(errored)))

            for num, err in errored:
                if err:
                    print('Chunk {} - Impacting Row Numbers: {} - {}'.format(num, err[0], err[-1]))
                else:
                    print('Error reporting failed to properly estimate impacted row numbers, please report this.')
        else:
            print('{} import successfully completed!'.format(file_name))

        file_num += 1

    if args.validate:
        validate_files(file_list=validation_files, warnings=args.warning, build_package=False, threads=args.worker_threads, config=config)


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
        print('This miNDAR has no tables yet. You can add one by executing \'mindar table add <schema> <table-name>\'.')
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


def main():
    args = parse_args()

    if args.profile:
        atexit.register(print_time_exit, start_time=time.time())

    config = load_config(args)
    mindar = MindarManager(config)
    args.func(args, config, mindar)  # execute selected argument function


if __name__ == '__main__':
    main()
