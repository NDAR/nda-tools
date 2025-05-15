import functools
import logging
from collections import defaultdict

from NDATools import exit_error
from NDATools.Utils import evaluate_yes_no_input
from NDATools.upload.submission.api import SubmissionApi, Submission, SubmissionDetails

logger = logging.getLogger(__name__)


def check_replacement_authorized(config, submission_id):
    api = SubmissionApi(config.submission_api_endpoint, config.username, config.password)
    submission_history = api.get_submission_history(submission_id)

    # check to see if the submission was already replaced?
    if not submission_history[0].replacement_authorized:
        if len(submission_history) > 1 and submission_history[1].replacement_authorized:
            message = '''Submission {} was already replaced by {} on {}.
    If you need to make further edits to this submission, please reach out the the NDA help desk''' \
                .format(submission_id, submission_history[0].created_by, submission_history[0].created_date)
            exit_error(message=message)
        else:
            exit_error(
                message='submission_id {} is not authorized to be replaced. Please contact the NDA help desk for approval to replace this submission'.format(
                    submission_id))


def _check_missing_data_for_resubmission(validated_files, submission_details: SubmissionDetails):
    def add(d, v):
        if hasattr(v, 'rows'):
            d[v.short_name] += v.rows
        elif hasattr(v, 'row_count'):
            d[v.short_name] += v.row_count
        else:
            raise Exception('Unexpected object type: {}'.format(v))
        return d

    # create a dictionary containing row counts per datastructure in validated_files
    provided_row_counts = functools.reduce(add, validated_files, defaultdict(int))

    # create a dictionary containing row counts per datastructure in original submission
    submission_row_counts = functools.reduce(add, submission_details.data_structure_details, defaultdict(int))

    # find structures where the number of rows provided is less than expected
    data_structures_with_missing_rows = []
    for short_name, row_count in submission_row_counts.items():
        # Users only need to submit data for structures with changed data, so dont flag structures that the user submit changes for.
        submitted_data = short_name in provided_row_counts
        if submitted_data and row_count > provided_row_counts[short_name]:
            data_structures_with_missing_rows.append((short_name, row_count, provided_row_counts[short_name]))

    # TODO need to accommodate non-interactive mode
    if data_structures_with_missing_rows:
        logger.warning('\nWARNING - Detected missing information in the following files: ')

        for tuple_expected_actual in data_structures_with_missing_rows:
            logger.warning(
                '\n{} - expected {} rows but found {}  '.format(tuple_expected_actual[0],
                                                                tuple_expected_actual[1],
                                                                tuple_expected_actual[2]))
        prompt = '\nIf you update your submission with these files, the missing data will be reflected in your data-expected numbers'
        prompt += '\nAre you sure you want to continue? (y/n): '
        proceed = evaluate_yes_no_input(prompt)
        if str(proceed).lower() == 'n':
            exit_error(message='')


def _check_unrecognized_datastructures(validated_files, submission_details: SubmissionDetails):
    short_names_provided = {v.short_name for v in validated_files}
    short_names_in_submission = {v.short_name for v in submission_details.data_structure_details}
    unrecognized_ds = short_names_provided.difference(short_names_in_submission)
    if unrecognized_ds:
        message = 'ERROR - The following datastructures were not included in the original submission and therefore cannot be included in the replacement submission: '
        message += "\r\n" + "\r\n".join(unrecognized_ds)
        exit_error(message=message)


class ReplacementPackageInfo:
    def __init__(self, submission_id, collection_id, title, description, validation_uuids):
        self.submission_id = submission_id
        self.collection_id = collection_id
        self.title = title
        self.description = description
        self.validation_uuids = validation_uuids


def build_replacement_package_info(validated_files, submission: Submission,
                                   submission_details: SubmissionDetails) -> ReplacementPackageInfo:
    # perform some checks before attempting to build the package
    _check_unrecognized_datastructures(validated_files, submission_details)
    _check_missing_data_for_resubmission(validated_files, submission_details)

    validation_uuid = _generate_uuids_for_qa_workflow(validated_files, submission_details)
    return ReplacementPackageInfo(submission.submission_id, submission.collection.id, submission.dataset_title,
                                  submission.dataset_description, validation_uuid)


def _generate_uuids_for_qa_workflow(validated_files, submission_details: SubmissionDetails):
    new_uuids = set(submission_details.validation_uuids)
    # create a dictionary containing validation-uuids per datastructure in validated_files
    validation_uuids_by_short_name = defaultdict(set)
    for file in validated_files:
        validation_uuids_by_short_name[file.short_name].add(file.uuid)

    # adds to 'new_uuids' one datastructure at a time
    for short_name in validation_uuids_by_short_name:
        structure_details = submission_details.get_data_structure_details(short_name)

        # remove uuids from the previous submission for this data-structure
        new_uuids = new_uuids.difference(set(structure_details.validation_uuids))
        # add uuids from the validated_files for this data-structure
        new_uuids.update(validation_uuids_by_short_name[short_name])

    return list(new_uuids)
