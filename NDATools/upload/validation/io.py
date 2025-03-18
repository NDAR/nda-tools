import logging

from tabulate import tabulate

from NDATools import NDA_TOOLS_VAL_FOLDER
from NDATools.Utils import exit_error
from NDATools.upload.validation.api import ValidationResponse
from NDATools.upload.validation.filewriter import JsonValidationFileWriter, CsvValidationFileWriter

logger = logging.getLogger(__name__)


def preview_validation_errors(results: [ValidationResponse], limit=10):
    table_list = []
    for result in results:
        r: ValidationResponse = result
        logger.info('\nErrors found in {}:'.format(r.file.name))
        errors = r.rw_creds.download_errors()
        # errors are grouped by error type, so we need to ungroup and flatten to display in a table by record.
        # add list splice to reduce memory footprint
        errors = [e for error_list in errors.values() for e in error_list[:limit]]
        rows = [
            [
                error['record'] if 'record' in error else '',
                error['columnName'] if 'columnName' in error else '',
                error['message']
            ] for error in errors[:limit]
        ]
        if rows:
            logger.info('')
            table = tabulate(rows, headers=['Row', 'Column', 'Message'])
            table_list.append(table)
            logger.info(table)
            logger.info('')
        if len(errors) > limit:
            logger.info('\n...and {} more errors'.format(len(errors) - limit))
    return table_list


class UserIO:
    def __init__(self, *, is_json, skip_prompt):
        self.file_writer = JsonValidationFileWriter(NDA_TOOLS_VAL_FOLDER) if is_json \
            else CsvValidationFileWriter(NDA_TOOLS_VAL_FOLDER)

    def run_validation_step_io(self, results: [ValidationResponse], output_warnings: bool):
        # Print out various information based on the command line args and the status of the validation results
        self.file_writer.write_errors(results)
        logger.info(
            '\nAll files have finished validating. Validation report output to: {}'.format(
                self.file_writer.errors_file))
        if any(map(lambda x: x.status == 'SystemError', results)):
            msg = 'Unexpected error occurred while validating one or more of the csv files.'
            msg += '\nPlease email NDAHelp@mail.nih.gov for help in resolving this error and include {} as an attachment to help us resolve the issue'
            exit_error(msg)

        if output_warnings:
            self.file_writer.write_warnings(results)
            logger.info('Warnings output to: {}'.format(self.file_writer.warnings_file))
        else:
            if any(map(lambda x: x.has_warnings(), results)):
                logger.info('Note: Your data has warnings. To save warnings, run again with -w argument.')

        errors = list(filter(lambda x: x.has_errors(), results))
        success = list(filter(lambda x: not x.has_errors(), results))
        if success:
            logger.info('The following files passed validation:')
            for result in success:
                logger.info('UUID {}: {}'.format(result.uuid, result.file.name))
        if errors:
            logger.info('\nThese files contain errors:')
            for result in errors:
                logger.info('UUID {}: {}'.format(result.uuid, result.file.name))
            preview_validation_errors(results, limit=10)
            if not success:
                exit_error('No files passed validation, please correct any errors and validate again.')
