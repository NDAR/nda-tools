import abc
import csv
import enum
import json
import logging
import os
import time
from collections import defaultdict
from typing import List

from NDATools import NDA_TOOLS_VAL_FOLDER
from NDATools.upload.cli import ValidatedFile, ValidationError, ManifestValidationError

logger = logging.getLogger(__name__)


class Extension(enum.Enum):
    JSON = '.json'
    CSV = '.csv'


def group_errors_by_key(errors: List[ValidationError]):
    obj = defaultdict(list)
    for error in errors:
        obj[error.err_code].append(error)
    return obj


class ResultsWriterABC(abc.ABC):
    def __init__(self, results_folder, ext: Extension):
        date = time.strftime("%Y%m%dT%H%M%S")
        self.errors_file = os.path.join(results_folder, f'validation_results_{date}{ext.value}')
        self.warnings_file = os.path.join(results_folder, f'validation_warnings_{date}{ext.value}')

    @abc.abstractmethod
    def write_errors(self, results: [ValidatedFile]) -> str:
        ...

    @abc.abstractmethod
    def write_warnings(self, results: [ValidatedFile]) -> str:
        ...


class JsonValidationResultsEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ValidationError):
            return {
                'recordNumber': obj.record_number,
                'columnName': obj.column_name,
                'message': obj.message
            }
        elif isinstance(obj, ManifestValidationError):
            return {
                'recordNumber': obj.manifest.record_number,
                'manifest': obj.manifest.name,
                'message': obj.message
            }
        return super().default(obj)


class JsonWriter(ResultsWriterABC):
    def __init__(self, results_folder):
        super().__init__(results_folder, Extension.JSON)

    def _write(self, results, is_errors):
        json_data = dict(Results=[])
        for result in results:
            r: ValidatedFile = result
            key = 'Errors' if is_errors else 'Warnings'
            # group errors/warnings by err_code
            obj = group_errors_by_key(r.errors if is_errors else r.warnings)

            # add in the manifest validation errors
            if is_errors and r.manifest_errors:
                obj['manifest_error'] = []
                for error in r.manifest_errors:
                    obj['manifest_error'].append(error)

            json_data['Results'].append({
                'File': r.file.name,
                'ID': r.uuid,
                'Status': r.status,
                'Expiration Date': '',
                key: obj
            })
        with open(self.errors_file if is_errors else self.warnings_file, 'w') as f:
            json.dump(json_data, f, cls=JsonValidationResultsEncoder)

    def write_errors(self, results: List[ValidatedFile]):
        self._write(results, True)
        return self.errors_file

    def write_warnings(self, results: List[ValidatedFile]):
        self._write(results, False)
        return self.warnings_file


class CsvWriter(ResultsWriterABC):
    def __init__(self, results_folder):
        super().__init__(results_folder, Extension.CSV)

    def write_errors(self, results: List[ValidatedFile]):
        fieldnames = ['FILE', 'ID', 'STATUS', 'EXPIRATION_DATE', 'ERRORS', 'COLUMN', 'MESSAGE', 'RECORD']
        with open(self.errors_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                r: ValidatedFile = result
                for error in sorted(r.errors, key=lambda x: x.record_number or 0):
                    row = {
                        'FILE': r.file.name,
                        'ID': r.uuid,
                        'STATUS': r.status,
                        'EXPIRATION_DATE': '',
                        'ERRORS': error.err_code,
                        'COLUMN': error.column_name,
                        'MESSAGE': error.message,
                        'RECORD': error.record_number
                    }
                    writer.writerow(row)
                for error in sorted(r.manifest_errors, key=lambda x: x.manifest.record_number or 0):
                    row = {
                        'FILE': r.file.name,
                        'ID': r.uuid,
                        'STATUS': r.status,
                        'EXPIRATION_DATE': '',
                        'ERRORS': 'manifest_error',
                        'COLUMN': error.manifest.column,
                        'MESSAGE': error.message,
                        'RECORD': error.manifest.record_number
                    }
                    writer.writerow(row)

                # if there are no errors in the file, write a single row to indicate no errors were found
                if not r.errors and not r.manifest_errors:
                    writer.writerow({
                        'FILE': r.file.name,
                        'ID': r.uuid,
                        'STATUS': r.status,
                        'EXPIRATION_DATE': '',
                        'ERRORS': 'None',
                        'COLUMN': 'None',
                        'MESSAGE': 'None',
                        'RECORD': 'None'
                    })
        return self.errors_file

    def write_warnings(self, results: List[ValidatedFile]):
        fieldnames = ['FILE', 'ID', 'STATUS', 'EXPIRATION_DATE', 'WARNINGS', 'MESSAGE', 'COUNT']
        with open(self.warnings_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                r: ValidatedFile = result
                for key, values in group_errors_by_key(r.warnings).items():
                    row = {
                        'FILE': r.file.name,
                        'ID': r.uuid,
                        'STATUS': r.status,
                        'EXPIRATION_DATE': '',
                        'WARNINGS': key,
                        # this is how this was done originally, though this doesnt make sense to me
                        'MESSAGE': values[0].message,
                        'COUNT': len(values)
                    }
                    writer.writerow(row)
                # if there are no warnings in the file, write a single row to indicate no warnings were found
                if not r.warnings:
                    writer.writerow({
                        'FILE': r.file.name,
                        'ID': r.uuid,
                        'STATUS': r.status,
                        'EXPIRATION_DATE': '',
                        'WARNINGS': 'None',
                        'MESSAGE': 'None',
                        'COUNT': '0'
                    })
        return self.warnings_file


class ResultsWriterFactory:

    @staticmethod
    def get_writer(file_format: str = 'csv'):
        if file_format == 'csv':
            return CsvWriter(NDA_TOOLS_VAL_FOLDER)
        elif file_format == 'json':
            return JsonWriter(NDA_TOOLS_VAL_FOLDER)
        else:
            raise NotImplementedError(f'format {file_format} not supported')
