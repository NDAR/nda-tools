import enum
from enum import Enum
from copy import copy

from NDATools.MindarManager import *
from NDATools.MindarHelpers import *
from NDATools.clientscripts.vtcmd import validate_files


__all__ = ['MindarSubmission', 'MindarSubmissionStep']
__incorrect_type__ = 'Incorrect type for {}, was expecting {}'


def export(mindar, table, schema, download_dir, **kwargs):
    print('    Exporting...')

    download_dir = get_export_dir(download_dir, schema)

    files = export_mindar_helper(mindar, [table], schema, download_dir, add_nda_header=True)

    print('    Export complete.')

    result = locals()
    result['download_dir'] = download_dir
    result['files'] = files

    return result


def validate(mindar, table, schema, files, warning, worker_threads, config, **kwargs):
    print('    Validating...')

    validation_uuid, associated_files = validate_files(file_list=files, warnings=warning, build_package=False,
                                                       threads=worker_threads, config=config)

    print('    Validation complete.')
    result = locals()
    result['validation_uuid'] = validation_uuid
    result['associated_files'] = associated_files

    return result


def create_submission_package(mindar, table, schema, validation_uuid, **kwargs):
    result = locals()

    return result


def create_submission(mindar, table, schema, submission_package, **kwargs):
    result = locals()

    return result


class WrappedFunction:

    def __init__(self, function):
        self._function = function

    def __call__(self, *args, **kwargs):
        return self._function(*args, **kwargs)


@enum.unique
class MindarSubmissionStep(Enum):
    SUBMISSION = (WrappedFunction(create_submission), ['mindar', 'table', 'schema', 'submission_package'])
    SUBMISSION_PACKAGE = (WrappedFunction(create_submission_package), ['mindar', 'table', 'schema'], 'SUBMISSION')
    VALIDATE = (WrappedFunction(validate), ['mindar', 'table', 'schema', 'download_dir'], 'SUBMISSION_PACKAGE')
    EXPORT = (WrappedFunction(export), ['mindar', 'table', 'schema', 'download_dir'], 'VALIDATE')

    def __new__(cls, wrapped_function, required_arguments, next_step=None):
        obj = object.__new__(cls)
        obj._value_ = wrapped_function
        obj._args = required_arguments
        obj._next = next_step

        return obj

    def __call__(self, **kwargs):
        for required in self._args:
            if required not in kwargs:
                raise SyntaxError('Invocation of MindarSubmissionStep {} halted due to missing required kwarg: {}'
                                  .format(self.name, required))

        return unpack_kwargs(self.value(**kwargs))

    def __str__(self):
        return '{}'.format(self.name)

    def has_next(self):
        return self._next is not None

    def next(self):
        return MindarSubmissionStep[self._next]


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

        self._schema = schema
        self._table = table
        self._step = step
        self._mindar = mindar_manager

    def __call__(self, **kwargs):
        if 'mindar' not in kwargs:
            kwargs['mindar'] = self._mindar

        if 'schema' not in kwargs:
            kwargs['schema'] = self._schema

        if 'table' not in kwargs:
            kwargs['table'] = self._table

        return self._step(**kwargs)

    def __iter__(self):
        return MindarSubmissionIterator(self)

    def __str__(self):
        return 'MindarSubmission[schema={}, table={}, step={}]'.format(self._schema, self._table, self._step)

    def set_step(self, step):
        if not isinstance(step, MindarSubmissionStep):
            raise ValueError(__incorrect_type__.format('step', 'MindarSubmissionStep'))

        self._step = step

    def get_step(self):
        return self._step


class MindarSubmissionIterator:

    def __init__(self, submission):
        if not isinstance(submission, MindarSubmission):
            raise ValueError(__incorrect_type__.format('submission', 'MindarSubmission'))

        self._submission = submission

    def __next__(self):
        if not self._submission:
            raise StopIteration

        result = copy(self._submission)

        if self._submission.get_step().has_next():
            self._submission.set_step(self._submission.get_step().next())
        else:
            self._submission = None

        return result
