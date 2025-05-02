from NDATools.BuildPackage import SubmissionPackage
from NDATools.Utils import get_request, exit_error
from NDATools.upload.submission.api import SubmissionApi


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


def build_replacement_package(validated_files, args, config) -> SubmissionPackage:
    # auth = requests.auth.HTTPBasicAuth(config.username, config.password)
    # response = get_request('/'.join([config.submission_api_endpoint, submission_id]), auth=auth)
    api = SubmissionApi(config)
    submission = api.get_submission(submission_id)

    submission_id = submission.submission_id
    config.title = submission.dataset_title
    config.description = submission.dataset_description
    config.collection_id = submission.collection.id

    # get pending-changes for submission-id
    # response = get_request('/'.join([config.submission_api_endpoint, submission_id, 'pending-changes']), auth=auth)
    submission_version = api.get_latest_submission_version(submission_id)

    # get list of associated-files that have already been uploaded for pending changes
    pending_changes = []
    original_submission_id = submission_id
    original_uuids = {uuid for uuid in response['validation_uuids']}
    for change in response['pendingChanges']:
        validation_uuids = change['validationUuids']
        manifest_files = []
        for uuid in validation_uuids:
            response = get_request('/'.join([config.validation_api_endpoint, uuid]))
            manifest_files.extend(manifest['localFileName'] for manifest in response['manifests'])
        change['manifests'] = manifest_files
        pending_changes.append(change)

    return pending_changes, original_uuids, original_submission_id


def _check_missing_data_for_resubmission(validated_files, pending_changes, original_uuids):
    if pending_changes:
        structure_to_new_row_count = {}
        for uuid in uuid_dict:
            short_name = uuid_dict[uuid]['short_name']
            structure_to_new_row_count[short_name] = 0
        for uuid in uuid_dict:
            short_name = uuid_dict[uuid]['short_name']
            structure_to_new_row_count[short_name] += uuid_dict[uuid]['rows']

        unrecognized_ds = set()
        data_structures_with_missing_rows = []
        for data_structure in structure_to_new_row_count:
            expected_change_for_data_structure = next(
                filter(lambda pending_change: pending_change['shortName'] == data_structure, pending_changes),
                None)
            if expected_change_for_data_structure is not None:
                if structure_to_new_row_count[data_structure] < expected_change_for_data_structure['rows']:
                    data_structures_with_missing_rows.append((data_structure,
                                                              expected_change_for_data_structure['rows'],
                                                              structure_to_new_row_count[data_structure]))
            else:
                unrecognized_ds.update({data_structure})

        # update list of validation-uuids to be used during the packaging step
        new_uuids, unrecognized_ds = _generate_uuids_for_qa_workflow(unrecognized_ds)

        if unrecognized_ds:
            message = 'ERROR - The following datastructures were not included in the original submission and therefore cannot be included in the replacement submission: '
            message += "\r\n" + "\r\n".join(unrecognized_ds)
            exit_error(message=message)
        else:
            data_structures_with_missing_rows = data_structures_with_missing_rows
            uuid = new_uuids


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
