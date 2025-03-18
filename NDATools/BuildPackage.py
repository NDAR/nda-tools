from NDATools.Configuration import *
from NDATools.Utils import *

logger = logging.getLogger(__name__)


class SubmissionPackage:
    def __init__(self, uuid, config, pending_changes=None, original_uuids=None):
        self.config = config
        self.api = self.config.submission_package_api
        self.validationtool_api = self.config.validationtool_api
        self.uuid = uuid
        self.username = self.config.username
        self.password = self.config.password
        self.dataset_name = self.config.title
        self.dataset_description = self.config.description
        self.package_info = {}
        self.package_id = None
        self.package_folder = None
        self.auth = requests.auth.HTTPBasicAuth(self.config.username, self.config.password)
        self.alt_endpoints = None
        self.collection_id = self.check_and_get_collection_id()
        self.validation_results = []
        self.pending_changes = pending_changes
        self.original_validation_uuids = original_uuids
        # these fields will be set after the package is created
        self.expiration_date = None
        self.create_date = None
        self.submission_package_uuid = None

    def get_collections(self):
        collections = get_request("/".join([self.validationtool_api, "user/collection"]), auth=self.auth,
                                  headers={'Accept': 'application/json'})
        return {int(c['id']): c['title'] for c in collections}

    def has_permissions_to_submit_to_collection(self, collection_id, user_collections):
        c_id = int(collection_id)
        if c_id in user_collections:
            return True
        # TODO refactor collection-api to remove the need to make these separate api calls
        if self.alt_endpoints is not None:
            return c_id in self.alt_endpoints
        else:
            endpoints = get_request("/".join([self.validationtool_api, "user/customEndpoints"]), auth=self.auth,
                                    headers={'Accept': 'application/json'})
            # TODO cache api response
            all_collections = get_request("/".join([self.config.collection_api]), auth=self.auth,
                                          headers={'Accept': 'application/json'})
            tmp_endpoints = {e['title'] for e in endpoints}
            self.alt_endpoints = {int(c['id']): c['title'] for c in all_collections if
                                  c['altEndpoint'] in tmp_endpoints}
            return c_id in self.alt_endpoints

    def check_and_get_collection_id(self):
        user_collections = self.get_collections()
        if not user_collections:
            message = 'The user {} does not have permission to submit to any collections.'.format(self.config.username)
            exit_error(message=message)
        if self.config.collection_id:
            if not self.has_permissions_to_submit_to_collection(self.config.collection_id, user_collections):
                message = 'The user {} does not have permission to submit to collection {}.'.format(
                    self.config.username, self.config.collection_id)
                exit_error(message=message)
            else:
                return self.config.collection_id
        else:
            return self.prompt_for_collection_id(user_collections)

    def prompt_for_collection_id(self, user_collections):
        while True:
            try:
                user_input = int(input('\nEnter collection ID:').strip())
                if not self.has_permissions_to_submit_to_collection(user_input, user_collections):
                    logger.error(f'You do not have access to submit to the collection: {user_input} ')
                    logger.info(f'Please choose from one of the following collections: ')
                    for collection_id in sorted(user_collections.keys()):
                        logger.info('{}: {}'.format(collection_id, user_collections[collection_id]))
                else:
                    return user_input
            except ValueError:
                logger.error('Error: Input must be a valid integer')

    def build_package(self):
        def raise_error(value):
            raise Exception("Missing {}. Please try again.".format(value))

        if not self.dataset_name:
            raise_error('dataset name')

        if not self.dataset_description:
            raise_error('dataset description')

        if self.collection_id is None:
            raise_error('collection ID')

        self.package_info = {
            "package_info": {
                "dataset_description": self.dataset_description,
                "dataset_name": self.dataset_name,
                "collection_id": self.collection_id
            },
            "validation_results":
                self.uuid
        }
        if self.config.replace_submission:
            self.package_info['package_info']['replacement_submission'] = self.config.replace_submission
            self.print_replacement_summary()
            if not self.config.force:
                user_input = evaluate_yes_no_input("Are you sure you want to continue?", 'n')
                if user_input.lower() == 'n':
                    exit_error(message='Exiting...')

        json_data = json.dumps(self.package_info)
        response = post_request(self.api, json_data, auth=self.auth)
        if response:
            try:
                self.package_id = response['submission_package_uuid']
                for r in response['validation_results']:
                    self.validation_results.append(r['id'])
                self.submission_package_uuid = str(response['submission_package_uuid'])
                self.create_date = str(response['created_date'])
                self.expiration_date = str(response['expiration_date'])
            except KeyError:
                message = 'There was an error creating your package.'
                if response['status'] == Status.ERROR:
                    message = response['errors'][0]['message']
                exit_error(message=message)

            while response['package_info']['status'] == Status.PROCESSING:
                time.sleep(1.1)
                response = get_request("/".join([self.api, self.package_id]), auth=self.auth)
            if response['package_info']['status'] != Status.COMPLETE:
                message = 'There was an error in building your package.'
                if response['package_info']['status'] == Status.SYSERROR:
                    message = response['errors']['system'][0]['message']
                elif 'has changed since validation' in response['errors']:
                    message = response['errors']
                exit_error(message=message)
        else:
            message = 'There was an error with your package request.'
            exit_error(message=message)

    def print_replacement_summary(self):
        logger.info('Below is a summary of what your submission will look like with the validation files provided:')
        logger.info('')
        logger.info('Short-Name, Number of Rows')
        for change in self.pending_changes:
            logger.info('{},{}'.format(change['shortName'], change['rows']))
        logger.info('')
        logger.info('')


class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'
    COMPLETE = 'complete'
    ERROR = 'error'
    PROCESSING = 'processing'
