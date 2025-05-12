import pathlib

from NDATools.Configuration import *
from NDATools.upload.submission.api import SubmissionPackageApi, PackagingStatus, SubmissionApi
from NDATools.upload.submission.associated_file import AssociatedFileUploader

logger = logging.getLogger(__name__)


class SubmissionPackage:
    def __init__(self, config):
        self.config = config
        self.api = SubmissionPackageApi(self.config.submission_package_api_endpoint, self.config.username,
                                        self.config.password)

    def build_package(self, validation_uuid, collection, name, description, replacement_submission_id=None):
        package = self.api.build_package(collection, name, description, validation_uuid, replacement_submission_id)
        if package.status == PackagingStatus.PROCESSING:
            self.api.wait_package_complete(package.submission_package_uuid)
        # print package info to console
        logger.info('\n\nPackage Information:')
        logger.info('validation results: {}'.format(validation_uuid))
        logger.info('submission_package_uuid: {}'.format(package.submission_package_uuid))
        logger.info('created date: {}'.format(package.created_date))
        logger.info('expiration date: {}'.format(package.expiration_date))
        return package.submission_package_uuid


class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'
    COMPLETE = 'Complete'
    ERROR = 'Error'
    PROCESSING = 'In Progress'
    READY = 'Ready'


class Submission:
    def __init__(self, config):
        self.config = config
        self.api = SubmissionApi(self.config.submission_api_endpoint, self.config.username, self.config.password)
        self.file_uploader = AssociatedFileUploader(self.api, config.worker_threads, config.force,
                                                    config.hide_progress,
                                                    self.config.batch_size)
        self.submission = None

    def replace_submission(self, submission_id, package_id):
        self.submission = self.api.replace_submission(submission_id, package_id)

    def submit(self, package_id):
        self.submission = self.api.create_submission(package_id)

    def resume_submission(self, submission_id):
        self.submission = self.api.get_submission(submission_id)

        if self.submission.status == Status.UPLOADING:
            self.upload_associated_files(resuming_upload=True)

    def upload_associated_files(self, resuming_upload=False):
        assert self.submission is not None, 'Must call submit/resume/replace before calling this method'
        associated_file_dirs = list(map(lambda x: pathlib.Path(x), self.config.directory_list or [os.getcwd()]))
        self.file_uploader.start_upload(self.submission, associated_file_dirs, resuming_upload)
        self.submission = self.api.get_submission(self.submission.submission_id)
