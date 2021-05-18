import enum
from enum import Enum
from copy import copy

from NDATools.clientscripts.vtcmd import build_package
from NDATools.clientscripts.vtcmd import submit_package
from NDATools.MindarManager import *
from NDATools.MindarHelpers import *
from NDATools.clientscripts.vtcmd import validate_files

__all__ = ['MindarSubmission']
__incorrect_type__ = 'Incorrect type for {}, was expecting {}'


class MindarSubmission:

    def __init__(self, schema, table, step, mindar_manager):
        if not isinstance(schema, str):
            raise ValueError(__incorrect_type__.format('schema', 'str'))

        if not isinstance(table, str):
            raise ValueError(__incorrect_type__.format('table', 'str'))

        if not isinstance(step, MindarSubmissionStep):
            raise ValueError(__incorrect_type__.format('step', 'MindarSubmissionStep'))

        if not isinstance(mindar_manager, MindarManager):
            raise ValueError(__incorrect_type__.format('mindar_manager', 'MindarManager'))

        self.schema = schema
        self.table = table
        self.step = step
        self.mindar = mindar_manager
        # export step
        self.files = None
        self.download_dir = None
        # validation step
        self.validation_uuid = None
        self.associated_files = None

    def __str__(self):
        return 'MindarSubmission[schema={}, table={}, step={}]'.format(self.schema, self.table, self.step)

    def set_step(self, step):
        if not isinstance(step, MindarSubmissionStep):
            raise ValueError(__incorrect_type__.format('step', 'MindarSubmissionStep'))

        self.step = step

    def get_remaining_steps(self):
        test = [s for s in MindarSubmissionStep if s.order >= self.step.order]
        test.sort(key=lambda x: x.order)
        return test

    def process(self, args, config):
        for s in self.get_remaining_steps():
            s.get_submission_proc()(self, args, config)

    def export(self, args, config):
        print('Exporting...')

        download_dir = get_export_dir(args.download_dir, args.schema)

        self.files = export_mindar_helper(self.mindar, [self.table], self.schema, download_dir, add_nda_header=True)

        if not self.files:
            raise Exception()

        print('Export complete.')

    def validate(self, args, config):
        print('Validating...')

        validation_uuid, associated_files = validate_files(file_list=self.files, warnings=args.warning, build_package=False,
                                                           threads=args.workerThreads, config=config)

        if not validation_uuid:
            raise Exception()

        # TODO - update status here
        print('Validation complete.')
        self.validation_uuid = validation_uuid
        self.associated_files = associated_files

    def create_submission_package(self, args, config):
        print('Starting Submission Package Step for miNDAR table {}...'.format(self.table))

        package_results = build_package(self.validation_uuid, self.associated_files, config=config)
        self.package_id = package_results[0]
        self.full_file_path = package_results[1]

        # TODO - update status here
        print('Submission-package Step complete.')


    def create_submission(self, args, config):
        print('Starting Submission Step for miNDAR table {}...'.format(self.table))
        submit_package(package_id=self.package_id, full_file_path=self.full_file_path, associated_files=self.associated_files,
                       threads=args.workerThreads, batch=args.batch, config=config)
        # TODO - update status here
        print('Starting Submission Step complete...')


    def initiate(self, args, config):
        # TODO - optionally update status here
        print('Starting Initiation step for miNDAR table {}...'.format(self.table))


@enum.unique
class MindarSubmissionStep(Enum):
    SUBMISSION_PACKAGE = (4, MindarSubmission.create_submission_package)
    VALIDATE = (3, MindarSubmission.validate)
    EXPORT = (2, MindarSubmission.export)
    SUBMISSION = (5, MindarSubmission.create_submission)
    INITIATE = (1, MindarSubmission.initiate)

    def __init__(self, order, wrapped_function):
        self.order = order
        self.submission_proc = wrapped_function

    def __str__(self):
        return '{}'.format(self.name)

    def has_next(self):
        return self._next is not None

    def get_submission_proc(self):
        return self.submission_proc

    def get_order(self):
        return self.order
