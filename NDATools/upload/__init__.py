import enum
import logging
import os
import pathlib
import traceback
from typing import List

from tabulate import tabulate

logger = logging.Logger(__name__)


class ManifestFile:
    def __init__(self, name: str, s3_destination: str, validated_file):
        self.name = name
        self.s3_destination = s3_destination
        self.validated_file = validated_file

    def upload(self, manifest_dir):
        try:
            self.validated_file.upload_manifest(self, manifest_dir)
        except Exception as e:
            if not isinstance(e, ManifestNotFoundError):
                logger.error(f'Unexpected error occurred while uploading {self.name}: {e}')
                logger.error(traceback.format_exc())
                raise ManifestUploadError(self, e)
            else:
                logger.debug(
                    f'Could not find manifest {self.name} from file {self.validated_file.file.name} in {manifest_dir}')
                raise e


class ManifestUploadError(Exception):
    def __init__(self, manifest: ManifestFile, unexpected_error: Exception = None):
        self.manifest = manifest
        self.error = unexpected_error


class ManifestNotFoundError(ManifestUploadError):
    ...


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
    def __init__(self, file: pathlib.Path, *, v1_resource=None, v2_resource=None, v2_creds=None):
        self.file = file
        assert v1_resource or v2_resource, "v1_resource or v2_resource must be specified"
        if v2_resource:
            self.status = ValidationStatus(v2_resource.status)
            self.uuid = v2_resource.uuid
            self._errors = None
            self._warnings = None
            self._manifests = None
            self._associated_files = None
            self._v2_creds = v2_creds
        elif v1_resource:
            self.status = ValidationStatus(v1_resource['status'])
            self.uuid = v1_resource['id']
            self._errors = v1_resource['errors']
            self._warnings = v1_resource['warnings']
            self._manifests = [ManifestFile(m['localFileName'], m['s3Destination'], self) for m in
                               v1_resource['manifests']]
            self._associated_files = v1_resource['associated_file_paths']

    def __hash__(self):
        return hash(self.file) + hash(self.uuid)

    def __eq__(self, other):
        if isinstance(other, ValidatedFile):
            return other.file == self.file and other.uuid == self.uuid
        return False

    @property
    def errors(self) -> List[ValidationError]:
        if self._errors:
            return list(map(lambda i: ValidationError(i.get('recordNumber'), i.get('columnName'), i.get('message')),
                            self._errors.items()))
        else:
            raise NotImplementedError()

    @property
    def manifests(self):
        if not self._manifests:
            self._manifests = [ManifestFile(m['local_file_name'], m['s3_destination']) for m in
                               self._v2_creds.download_manifests()]
        return self._manifests

    def has_manifest_errors(self):
        pass

    def is_valid(self):
        return not self.has_errors()

    def is_invalid(self):
        return not self.is_valid()

    def system_error(self):
        return self.status == ValidationStatus.SYSTEM_ERROR

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

    def show_manifest_errors(self):
        raise NotImplementedError()

    def upload_manifest(self, manifest: ManifestFile, manifest_dir: str):
        assert self._v2_creds
        local_file = pathlib.Path(os.path.join(manifest_dir, manifest.name))
        if not local_file.exists():
            raise ManifestNotFoundError(manifest)
        self._v2_creds.upload(str(local_file), manifest.s3_destination)
