import enum
import logging
import pathlib
from typing import List

from tabulate import tabulate

logger = logging.Logger(__name__)


class ValidationError:
    def __init__(self, record_number, column_name, message):
        self.record_number = record_number
        self.column_name = column_name
        self.message = message


class ValidationStatus(str, enum.Enum):
    PROCESSING = 'Processing'
    COMPLETE = 'Complete'
    COMPLETE_WITH_WARNINGS = 'CompleteWithWarnings'
    COMPLETE_WITH_ERRORS = 'CompleteWithErrors'
    SYSTEM_ERROR = 'SystemError'
    PENDING_MANIFESTS = 'PendingManifestFiles'


class ValidatedFile:
    def __init__(self, file: pathlib.Path, *, v1_resource=None, v2_resource=None):
        self.file = file
        assert v1_resource or v2_resource, "v1_resource or v2_resource must be specified"
        if v2_resource:
            self.status = ValidationStatus(v2_resource.status)
            self.uuid = v2_resource.uuid
            self._errors = None
            self._warnings = None
            self._manifests = None
            self._associated_files = None
            self.data_structures_with_missing_rows = None
        elif v1_resource:
            self.status = ValidationStatus(v1_resource['status'])
            self.uuid = v1_resource['id']
            self._errors = v1_resource['errors']
            self._warnings = v1_resource['warnings']
            self._manifests = v1_resource['manifests']
            self._associated_files = v1_resource['associated_file_paths']
            self.data_structures_with_missing_rows = None

    @property
    def errors(self) -> List[ValidationError]:
        if self._errors:
            return list(map(lambda i: ValidationError(i.get('recordNumber'), i.get('columnName'), i.get('message')),
                            self._errors.items()))
        else:
            raise NotImplementedError()

    @property
    def manifests(self) -> List[ValidationManifest]:
        this = self
        return list(
            map(lambda x: ValidationManifest(**{**x, 'validation_response': this}), self.rw_creds.download_manifests()))

    def is_valid(self):
        return not self.has_errors()

    def is_invalid(self):
        return not self.is_valid()

    def has_warnings(self):
        # return 'warnings' in str(self.status).lower()
        return self.status == ValidationStatus.COMPLETE_WITH_WARNINGS

    def has_errors(self):
        # return 'errors' in str(self.status).lower()
        return self.status == ValidationStatus.COMPLETE_WITH_ERRORS

    def waiting_manifest_upload(self):
        # return 'pending' in str(self.status).lower()
        return self.status == ValidationStatus.PENDING_MANIFESTS

    def preview_validation_errors(self, limit=10):
        table_list = []
        logger.info('\nErrors found in {}:'.format(self.file.name))
        # errors are grouped by error type, so we need to ungroup and flatten to display in a table by record.
        # add list splice to reduce memory footprint
        errors = self.errors
        rows = [
            [
                error.record_number,
                error.column_name,
                error.message
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
