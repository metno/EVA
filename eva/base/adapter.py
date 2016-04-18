import re
import uuid

import eva
import eva.logger
import eva.job
import eva.exceptions

import productstatus


class BaseAdapter(eva.ConfigurableObject):
    """!
    Adapters contain all the information and configuration needed to translate
    a Productstatus resource into job execution.
    """

    # @brief Common configuration variables all subclasses may use.
    _COMMON_ADAPTER_CONFIG = {
        'EVA_INPUT_DATA_FORMAT': {
            'type': 'list_string',
            'help': 'Comma-separated input Productstatus data format slugs',
            'default': '',
        },
        'EVA_INPUT_PARTIAL': {
            'type': 'null_bool',
            'help': 'Whether or not to process partial data instances',
            'default': 'NO',
        },
        'EVA_INPUT_PRODUCT': {
            'type': 'list_string',
            'help': 'Comma-separated input Productstatus product slugs',
            'default': '',
        },
        'EVA_INPUT_SERVICE_BACKEND': {
            'type': 'list_string',
            'help': 'Comma-separated input Productstatus service backend slugs',
            'default': '',
        },
        'EVA_INPUT_REFERENCE_HOURS': {
            'type': 'list_int',
            'help': 'Comma-separated reference hours to process data for',
            'default': '',
        },
        'EVA_OUTPUT_BASE_URL': {
            'type': 'string',
            'help': 'Base URL for DataInstances posted to Productstatus',
            'default': '',
        },
        'EVA_OUTPUT_DATA_FORMAT': {
            'type': 'string',
            'help': 'Productstatus Data Format ID for the finished product',
            'default': '',
        },
        'EVA_OUTPUT_FILENAME_PATTERN': {
            'type': 'string',
            'help': 'strftime pattern for output data instance filename',
            'default': '',
        },
        'EVA_OUTPUT_LIFETIME': {
            'type': 'int',
            'help': 'Lifetime of output data instance, in hours, before it can be deleted',
            'default': '',
        },
        'EVA_OUTPUT_PRODUCT': {
            'type': 'string',
            'help': 'Productstatus Product ID for the finished product',
            'default': '',
        },
        'EVA_OUTPUT_SERVICE_BACKEND': {
            'type': 'string',
            'help': 'Productstatus Service Backend ID for the position of the finished product',
            'default': '',
        },
        'EVA_PRODUCTSTATUS_API_KEY': {
            'type': 'string',
            'help': 'Productstatus API key',
            'default': '',
        },
        'EVA_PRODUCTSTATUS_USERNAME': {
            'type': 'string',
            'help': 'Productstatus user name',
            'default': '',
        },
    }

    _OPTIONAL_CONFIG = [
        'EVA_PRODUCTSTATUS_API_KEY',
        'EVA_PRODUCTSTATUS_USERNAME',
    ]

    PROCESS_PARTIAL_ONLY = 0
    PROCESS_PARTIAL_NO = 1
    PROCESS_PARTIAL_BOTH = 2

    def __init__(self, environment_variables, executor, api, logger, zookeeper):
        """!
        @param id an identifier for the adapter; must be constant across program restart
        @param api Productstatus API object
        @param environment_variables Dictionary of EVA_* environment variables
        """
        self.CONFIG = dict(self.CONFIG.items() + self._COMMON_ADAPTER_CONFIG.items())
        self.OPTIONAL_CONFIG = self.OPTIONAL_CONFIG + self._OPTIONAL_CONFIG
        self.logger = logger
        self.zookeeper = zookeeper
        self.executor = executor
        self.api = api
        self.env = environment_variables
        self.blacklist = set()
        self.required_uuids = set()
        self.template = eva.template.Environment()
        self.read_configuration()
        self.setup_process_partial()
        self.init()

    def setup_process_partial(self):
        """!
        @brief Set up the `process_partial` variable.
        """
        if 'EVA_INPUT_PARTIAL' not in self.env:
            self.process_partial = self.PROCESS_PARTIAL_NO
        elif self.env['EVA_INPUT_PARTIAL'] is None:
            self.process_partial = self.PROCESS_PARTIAL_BOTH
        elif self.env['EVA_INPUT_PARTIAL'] is True:
            self.process_partial = self.PROCESS_PARTIAL_ONLY
        else:
            self.process_partial = self.PROCESS_PARTIAL_NO

    def in_array_or_empty(self, data, env):
        """!
        @brief Filter input events by filter list. If a filter is not defined,
        then it is skipped and treated as matching. If a filter array is empty,
        it is also treated as matching.
        @returns True if `env` is not found in `self.env`, or if
        `self.env[env]` is empty, or if `env` is found in `self.env`.
        """
        if env not in self.env:
            return True
        return eva.in_array_or_empty(data, self.env[env])

    def blacklist_add(self, uuid):
        """!
        @brief Add a resource UUID to the blacklist. Once added to the
        blacklist, no ProductInstance with this UUID will be processed.
        """
        self.blacklist.add(uuid)

    def is_blacklisted(self, uuid):
        """!
        @returns True if a resource UUID is blacklisted, False otherwise.
        """
        return uuid in self.blacklist

    def forward_to_uuid(self, uuid):
        """!
        @brief Instruct the adapter to ignore messages that do not refer to the
        specified UUID through either its own ID or any child or parent IDs.
        This parameter is deleted once a valid message is accepted for
        processing.
        """
        self.required_uuids.add(uuid)

    def remove_required_uuid(self, uuid):
        """!
        @brief Delete an UUID from the required UUID set.
        """
        self.required_uuids.remove(uuid)

    def clear_required_uuids(self):
        """!
        @brief Delete all UUIDs from the required UUID set.
        """
        self.required_uuids.clear()

    def is_in_required_uuids(self, uuid):
        """!
        @returns True if self.required_uuids is empty or the specified UUID is
        in that set.
        """
        return (len(self.required_uuids) == 0) or (uuid in self.required_uuids)

    def datainstance_has_required_uuids(self, datainstance):
        """!
        @returns True if the DataInstance or any of its parents or children
        have the specified UUID as their primary key.
        """
        if self.is_in_required_uuids(datainstance.id):
            return True
        if self.is_in_required_uuids(datainstance.data.id):
            return True
        if self.is_in_required_uuids(datainstance.data.productinstance.id):
            return True
        if self.is_in_required_uuids(datainstance.data.productinstance.product.id):
            return True
        if self.is_in_required_uuids(datainstance.format.id):
            return True
        if self.is_in_required_uuids(datainstance.servicebackend.id):
            return True
        return False

    def resource_matches_input_config(self, resource):
        """!
        @brief Check that a Productstatus resource matches the configured
        processing criteria.
        """
        if resource._collection._resource_name != 'datainstance':
            self.logger.info('Resource is not of type DataInstance, ignoring.')

        elif not self.in_array_or_empty(resource.data.productinstance.product.slug, 'EVA_INPUT_PRODUCT'):
            self.logger.info('DataInstance belongs to Product "%s", ignoring.',
                             resource.data.productinstance.product.name)

        elif not self.in_array_or_empty(resource.servicebackend.slug, 'EVA_INPUT_SERVICE_BACKEND'):
            self.logger.info('DataInstance is hosted on service backend %s, ignoring.',
                             resource.servicebackend.name)

        elif not self.in_array_or_empty(resource.format.slug, 'EVA_INPUT_DATA_FORMAT'):
            self.logger.info('DataInstance file type is %s, ignoring.',
                             resource.format.name)
        elif not self.in_array_or_empty(resource.data.productinstance.reference_time.strftime('%H'), 'EVA_INPUT_REFERENCE_HOURS'):
            self.logger.info('DataInstance reference hour does not match any of %s, ignoring.', list(set(self.env['EVA_INPUT_REFERENCE_HOURS'])))
        elif resource.deleted:
            self.logger.info('DataInstance is marked as deleted, ignoring.')
        elif resource.partial and self.process_partial == self.PROCESS_PARTIAL_NO and not productstatus.datainstance_has_complete_file_count(resource):
            self.logger.info('DataInstance is not complete, ignoring.')
        elif resource.partial and self.process_partial == self.PROCESS_PARTIAL_ONLY and productstatus.datainstance_has_complete_file_count(resource):
            self.logger.info('DataInstance is complete, ignoring.')
        elif self.is_blacklisted(resource.id):
            self.logger.info('DataInstance %s is blacklisted, ignoring.', resource)
        elif self.is_blacklisted(resource.data.id):
            self.logger.info('Data %s is blacklisted, ignoring.', resource.data)
        elif self.is_blacklisted(resource.data.productinstance.id):
            self.logger.info('ProductInstance %s is blacklisted, ignoring.', resource.data.productinstance)
        elif not self.datainstance_has_required_uuids(resource):
            self.logger.info('DataInstance %s does not have any relationships to required UUIDs %s, ignoring.', resource.data.productinstance, list(self.required_uuids))
        else:
            self.clear_required_uuids()
            self.logger.info('DataInstance matches all configured criteria.')
            return True

        return False

    def print_datainstance_info(self, datainstance):
        """!
        @brief Print information about a DataInstance to the debug log.
        """
        self.logger.debug('Product: %s', datainstance.data.productinstance.product.name)
        self.logger.debug('Reference time: %s', eva.strftime_iso8601(datainstance.data.productinstance.reference_time))
        self.logger.debug('Time step: from %s to %s', eva.strftime_iso8601(datainstance.data.time_period_begin), eva.strftime_iso8601(datainstance.data.time_period_end))
        self.logger.debug('Data format: %s', datainstance.format.name)
        self.logger.debug('Service backend: %s', datainstance.servicebackend.name)

    def validate_and_process_resource(self, message_id, resource):
        """!
        @brief Check if the Resource fits this adapter, and send it to `process_resource`.
        @param resource A Productstatus resource.
        """
        if resource._collection._resource_name == 'datainstance':
            self.print_datainstance_info(resource)
        if not self.resource_matches_input_config(resource):
            return
        self.message_id = message_id
        self.logger.info('Start processing resource: %s', resource)
        self.process_resource(message_id, resource)
        self.logger.info('Finish processing resource: %s', resource)

    def process_resource(self, message_id, resource):
        """!
        @brief Perform any action based on a Productstatus resource. The
        resource is guaranteed to be validated against input configuration.
        @param resource A Productstatus resource.
        """
        raise NotImplementedError()

    def init(self):
        """!
        @brief This function provides a place for subclasses to initialize itself before accepting jobs.
        """
        pass

    def execute(self, job):
        """!
        @brief Execute a job with the assigned Executor.
        """
        return self.executor.execute(job)

    def has_productstatus_credentials(self):
        """!
        @return True if the adapter is configured with a user name and API key
        to Productstatus, False otherwise.
        """
        return (
            (len(self.env['EVA_PRODUCTSTATUS_USERNAME']) > 0) and
            (len(self.env['EVA_PRODUCTSTATUS_API_KEY']) > 0)
        )

    def require_productstatus_credentials(self):
        """!
        @brief Raise an exception if Productstatus credentials are not configured.
        """
        if not self.has_productstatus_credentials():
            raise eva.exceptions.MissingConfigurationException(
                'Posting to Productstatus requires environment variables EVA_PRODUCTSTATUS_USERNAME and EVA_PRODUCTSTATUS_API_KEY.'
            )
