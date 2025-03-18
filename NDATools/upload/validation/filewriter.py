import abc
import csv
import enum
import json
import logging
import os
import time

from NDATools.upload.validation.api import ValidationResponse

logger = logging.getLogger(__name__)


class Extension(enum.Enum):
    JSON = '.json'
    CSV = '.csv'


class ValidationFileWriter(abc.ABC):
    def __init__(self, results_folder, ext: Extension):
        date = time.strftime("%Y%m%dT%H%M%S")
        self.errors_file = os.path.join(results_folder, f'validation_results_{date}{ext.value}')
        self.warnings_file = os.path.join(results_folder, f'validation_warnings_{date}{ext.value}')

    @abc.abstractmethod
    def write_errors(self, results: [ValidationResponse]):
        ...

    @abc.abstractmethod
    def write_warnings(self, results: [ValidationResponse]):
        ...


class JsonValidationFileWriter(ValidationFileWriter):
    def __init__(self, results_folder):
        super().__init__(results_folder, Extension.JSON)

    def _write(self, results, is_errors):
        json_data = dict(Results=[])
        for result in results:
            r: ValidationResponse = result
            key = 'Errors' if is_errors else 'Warnings'
            json_data['Results'].append({
                'File': r.file.name,
                'ID': r.uuid,
                'Status': r.status,
                'Expiration Date': '',
                key: r.rw_creds.download_errors() if is_errors else r.rw_creds.download_warnings()
            })
        with open(self.errors_file if is_errors else self.warnings_file, 'w') as f:
            json.dump(json_data, f)

    def write_errors(self, results: [ValidationResponse]):
        self._write(results, True)

    def write_warnings(self, results: [ValidationResponse]):
        self._write(results, False)


class CsvValidationFileWriter(ValidationFileWriter):
    def __init__(self, results_folder):
        super().__init__(results_folder, Extension.CSV)

    def write_errors(self, results: [ValidationResponse]):
        fieldnames = ['FILE', 'ID', 'STATUS', 'EXPIRATION_DATE', 'ERRORS', 'COLUMN', 'MESSAGE', 'RECORD']
        with open(self.errors_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                r: ValidationResponse = result
                errors = r.rw_creds.download_errors()
                for error_key in errors.keys():
                    for error in errors[error_key]:
                        row = {
                            'FILE': r.file.name,
                            'ID': r.uuid,
                            'STATUS': r.status,
                            'EXPIRATION_DATE': '',
                            'ERRORS': error_key,
                            'COLUMN': error['columnName'] if 'columnName' in error else None,
                            'MESSAGE': error['message'],  # guaranteed to be in error
                            'RECORD': error['record'] if 'record' in error else None
                        }
                        writer.writerow(row)
                # if there are no errors in the file, write a single row to indicate no errors were found
                if not errors:
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

    def write_warnings(self, results: [ValidationResponse]):
        fieldnames = ['FILE', 'ID', 'STATUS', 'EXPIRATION_DATE', 'WARNINGS', 'MESSAGE', 'COUNT']
        with open(self.warnings_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                r: ValidationResponse = result
                warnings = r.rw_creds.download_warnings()
                for warning_key, values in warnings.items():
                    row = {
                        'FILE': r.file.name,
                        'ID': r.uuid,
                        'STATUS': r.status,
                        'EXPIRATION_DATE': '',
                        'WARNINGS': warning_key,
                        # this is how this was done originally, though this doesnt make sense to me
                        'MESSAGE': values[0]['message'],
                        'COUNT': len(values)
                    }
                    writer.writerow(row)
                # if there are no warnings in the file, write a single row to indicate no warnings were found
                if not warnings:
                    writer.writerow({
                        'FILE': r.file.name,
                        'ID': r.uuid,
                        'STATUS': r.status,
                        'EXPIRATION_DATE': '',
                        'WARNINGS': 'None',
                        'MESSAGE': 'None',
                        'COUNT': '0'
                    })
