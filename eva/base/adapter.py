import re
import uuid

import eva
import eva.logger
import eva.job
import eva.exceptions


class BaseAdapter(eva.ConfigurableObject):
    """!
    Adapters contain all the information and configuration needed to translate
    a Productstatus resource into job execution.
    """

    # @brief Common configuration variables all subclasses may use.
    _COMMON_ADAPTER_CONFIG = {
        'EVA_INPUT_DATA_FORMAT_UUID': 'Comma-separated input Productstatus data format UUIDs',
        'EVA_INPUT_PARTIAL': 'Whether or not to process partial data instances (allowed values are yes, no, and both; defaults to no)',
        'EVA_INPUT_PRODUCT_UUID': 'Comma-separated input Productstatus product UUIDs',
        'EVA_INPUT_SERVICE_BACKEND_UUID': 'Comma-separated input Productstatus service backend UUIDs',
        'EVA_OUTPUT_BASE_URL': 'Base URL for DataInstances posted to Productstatus',
        'EVA_OUTPUT_DATA_FORMAT_UUID': 'Productstatus Data Format UUID for the finished product',
        'EVA_OUTPUT_FILENAME_PATTERN': 'strftime pattern for output data instance filename',
        'EVA_OUTPUT_LIFETIME': 'Lifetime of output data instance, in hours, before it can be deleted',
        'EVA_OUTPUT_PRODUCT_UUID': 'Productstatus Product UUID for the finished product',
        'EVA_OUTPUT_SERVICE_BACKEND_UUID': 'Productstatus Service Backend UUID for the position of the finished product',
        'EVA_PRODUCTSTATUS_API_KEY': 'Productstatus API key',
        'EVA_PRODUCTSTATUS_USERNAME': 'Productstatus user name',
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
        self.process_partial = self.PROCESS_PARTIAL_NO
        self.read_input_partial_config()
        self.normalize_config_uuids()
        self.validate_configuration()
        self.init()

    def read_input_partial_config(self):
        if 'EVA_INPUT_PARTIAL' not in self.env:
            return
        partial = eva.parse_boolean_string(self.env['EVA_INPUT_PARTIAL'])
        if partial is True:
            self.process_partial = self.PROCESS_PARTIAL_ONLY
        elif partial is False:
            self.process_partial = self.PROCESS_PARTIAL_NO
        elif partial is None and self.env['EVA_INPUT_PARTIAL'] == 'both':
            self.process_partial = self.PROCESS_PARTIAL_BOTH
        else:
            raise eva.exceptions.InvalidConfigurationException('Invalid value for EVA_INPUT_PARTIAL: %s, expected one of "yes", "no", or "both"' % self.env['EVA_INPUT_PARTIAL'])

    def normalize_config_uuids(self):
        """!
        @brief Converts comma-separated configuration variable UUIDs starting
        with "EVA_INPUT_" to lists of UUIDs, and check that the UUID formats
        are valid.
        """
        errors = 0
        uuid_regex = re.compile('^EVA_\w+_UUID$')
        uuid_input_regex = re.compile('^EVA_INPUT_\w+_UUID$')
        for key, value in self.env.iteritems():
            if key not in self.REQUIRED_CONFIG and key not in self.OPTIONAL_CONFIG:
                continue
            if self.env[key] is None:
                continue
            if uuid_input_regex.match(key):
                self.env[key] = eva.split_comma_separated(self.env[key])
                uuids = self.env[key]
            elif uuid_regex.match(key):
                uuids = [self.env[key]]
            else:
                continue
            for id in uuids:
                try:
                    uuid.UUID(id)
                except ValueError, e:
                    self.logger.critical("Invalid UUID '%s' in configuration variable %s: %s" % (id, key, e))
                    errors += 1
        if errors > 0:
            raise eva.exceptions.InvalidConfigurationException('%d errors occurred during UUID normalization' % errors)

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

        elif not self.in_array_or_empty(resource.data.productinstance.product.id, 'EVA_INPUT_PRODUCT_UUID'):
            self.logger.info('DataInstance belongs to Product "%s", ignoring.',
                             resource.data.productinstance.product.name)

        elif not self.in_array_or_empty(resource.servicebackend.id, 'EVA_INPUT_SERVICE_BACKEND_UUID'):
            self.logger.info('DataInstance is hosted on service backend %s, ignoring.',
                             resource.servicebackend.name)

        elif not self.in_array_or_empty(resource.format.id, 'EVA_INPUT_DATA_FORMAT_UUID'):
            self.logger.info('DataInstance file type is %s, ignoring.',
                             resource.format.name)
        elif resource.deleted:
            self.logger.info('DataInstance is marked as deleted, ignoring.')
        elif resource.partial and self.process_partial == self.PROCESS_PARTIAL_NO:
            self.logger.info('DataInstance is marked as partial, ignoring.')
        elif not resource.partial and self.process_partial == self.PROCESS_PARTIAL_ONLY:
            self.logger.info('DataInstance is not marked as partial, ignoring.')
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

    def validate_and_process_resource(self, message_id, resource):
        """!
        @brief Check if the Resource fits this adapter, and send it to `process_resource`.
        @param resource A Productstatus resource.
        """
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
            ('EVA_PRODUCTSTATUS_USERNAME' in self.env) and
            ('EVA_PRODUCTSTATUS_API_KEY' in self.env) and
            (self.env['EVA_PRODUCTSTATUS_USERNAME'] is not None) and
            (self.env['EVA_PRODUCTSTATUS_API_KEY'] is not None)
        )

    def require_productstatus_credentials(self):
        """!
        @brief Raise an exception if Productstatus credentials are not configured.
        """
        if not self.has_productstatus_credentials():
            raise eva.exceptions.MissingConfigurationException(
                'Posting to Productstatus requires environment variables EVA_PRODUCTSTATUS_USERNAME and EVA_PRODUCTSTATUS_API_KEY.'
            )
