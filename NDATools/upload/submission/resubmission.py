import functools
import logging
from typing import List

from NDATools.BuildPackage import SubmissionPackage
from NDATools.Utils import exit_error, evaluate_yes_no_input
from NDATools.upload.cli import ValidatedFile
from NDATools.upload.submission.api import SubmissionApi, Submission, SubmissionDetails
from NDATools.upload.validation.v1 import ValidationV1Api

logger = logging.getLogger(__name__)


def check_replacement_authorized(config, submission_id):
    api = SubmissionApi(config)
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


'''
    pending_changes, original_uuids, original_submission_id = retrieve_replacement_submission_params(config,
                                                                                                         args.replace_submission)
    # For resubmission workflow: alert user if data loss was detected in one of their data-structures
    if original_submission_id and not config.force:
        data_structures_with_missing_rows = check_missing_data_for_resubmission(validated_files,
                                                                                pending_changes,
                                                                                original_uuids)
        if data_structures_with_missing_rows:
            logger.warning('\nWARNING - Detected missing information in the following files: ')

            for tuple_expected_actual in data_structures_with_missing_rows:
                logger.warning(
                    '\n{} - expected {} rows but found {}  '.format(tuple_expected_actual[0],
                                                                    tuple_expected_actual[1],
                                                                    tuple_expected_actual[2]))
            prompt = '\nIf you update your submission with these files, the missing data will be reflected in your data-expected numbers'
            prompt += '\nAre you sure you want to continue? <Yes/No>: '
            proceed = evaluate_yes_no_input(prompt, 'n')
            if str(proceed).lower() == 'n':
                exit_error(message='')
'''


def _check_missing_data_for_resubmission(validated_files: List[ValidatedFile], submission_details: SubmissionDetails):
    def add(d, v):
        if v.short_name not in d:
            d[v.short_name] = 0
        if isinstance(v, ValidatedFile):
            d[v.short_name] += v.row_count
        else:
            d[v.short_name] += v.rows
        return {**d}

    sorted_files = sorted(validated_files, key=lambda v: v.short_name)
    provided_row_counts = functools.reduce(add, sorted_files, dict())

    sorted_details = sorted(submission_details.data_structure_details, key=lambda v: v.short_name)
    submission_row_counts = functools.reduce(add, sorted_details, dict())

    data_structures_with_missing_rows = []
    for short_name, row_count in submission_row_counts.items():
        if row_count < provided_row_counts[short_name]:
            data_structures_with_missing_rows.append((short_name, row_count, provided_row_counts[short_name]))

    if data_structures_with_missing_rows:
        logger.warning('\nWARNING - Detected missing information in the following files: ')

        for tuple_expected_actual in data_structures_with_missing_rows:
            logger.warning(
                '\n{} - expected {} rows but found {}  '.format(tuple_expected_actual[0],
                                                                tuple_expected_actual[1],
                                                                tuple_expected_actual[2]))
        prompt = '\nIf you update your submission with these files, the missing data will be reflected in your data-expected numbers'
        prompt += '\nAre you sure you want to continue? <Yes/No>: '
        proceed = evaluate_yes_no_input(prompt, 'n')
        if str(proceed).lower() == 'n':
            exit_error(message='')


def _check_unrecognized_datastructures(validated_files: List[ValidatedFile], submission_details: SubmissionDetails):
    short_names_provided = {v.short_name for v in validated_files}
    short_names_in_submission = {v.short_name for v in submission_details.data_structure_details}
    unrecognized_ds = short_names_provided.difference(short_names_in_submission)
    if unrecognized_ds:
        message = 'ERROR - The following datastructures were not included in the original submission and therefore cannot be included in the replacement submission: '
        message += "\r\n" + "\r\n".join(unrecognized_ds)
        exit_error(message=message)


def build_replacement_package(validated_files, args, config) -> SubmissionPackage:
    submission_api = SubmissionApi(config)
    validation_api = ValidationV1Api(config.validation_api_endpoint)

    submission: Submission = submission_api.get_submission(args.replacement_submission)

    submission_id = submission.submission_id
    config.title = submission.dataset_title
    config.description = submission.dataset_description
    config.collection_id = submission.collection.id

    submission_details = submission_api.get_submission_details(submission_id)
    # perform some checks before attempting to build the package
    _check_unrecognized_datastructures(validated_files, submission_details)
    _check_missing_data_for_resubmission(validated_files, submission_details)

    # get list of associated-files that have already been uploaded for each data-structure
    # for structure_details in submission_details.data_structure_details:
    #     validation_uuids = structure_details.validation_uuids
    #     manifest_files = []
    #     for uuid in validation_uuids:
    #         v = validation_api.get_validation(uuid)
    #         manifest_files.extend(manifest.local_file_name for manifest in v.manifests)

    # return pending_changes, original_uuids, original_submission_id


def _generate_uuids_for_qa_workflow(unrecognized_ds=set()):
    unrecognized_structures = set(unrecognized_ds)
    new_uuids = set(original_uuids)
    val_by_short_name = {}
    for uuid in uuid_dict:
        short_name = uuid_dict[uuid]['short_name']
        val_by_short_name[short_name] = set()
    for uuid in uuid_dict:
        short_name = uuid_dict[uuid]['short_name']
        val_by_short_name[short_name].update({uuid})
    for short_name in val_by_short_name:
        # find the pending change with the same short name
        matching_change = {}
        for change in pending_changes:
            if change['shortName'] == short_name:
                matching_change = change
        if not matching_change:
            unrecognized_structures.add(short_name)
        else:
            # prevValidationUuids is the set of validation-uuids on the pending changes resource
            new_uuids = new_uuids.difference(set(matching_change['validationUuids']))
            new_uuids.update({res for res in val_by_short_name[short_name]})

    return list(new_uuids), unrecognized_structures
