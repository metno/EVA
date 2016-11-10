import os
import datetime
import logging
import kazoo.exceptions

import eva
import eva.config
import eva.globe
import eva.logger
import eva.job
import eva.exceptions

import productstatus
import productstatus.api


class BaseAdapter(eva.config.ConfigurableObject, eva.globe.GlobalMixin):
    """!
    Adapters contain all the information and configuration needed to translate
    a Productstatus resource into job execution.
    """

    # @brief Common configuration variables all subclasses may use.
    _COMMON_ADAPTER_CONFIG = {
        'executor': {
            'type': 'config_class',
            'help': 'Executor name from configuration files',
            'default': '',
        },
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
        'EVA_INPUT_WITH_HASH': {
            'type': 'null_bool',
            'help': 'Whether or not to process DataInstance resources containing a hash',
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
        'EVA_REFERENCE_TIME_THRESHOLD': {
            'type': 'int',
            'help': 'If non-zero, EVA will never process DataInstance resources that belong to a ProductInstance with a reference time older than N seconds.',
            'default': '0',
        },
        'EVA_SINGLE_INSTANCE': {
            'type': 'bool',
            'help': 'Allow only one EVA instance with the same group id running at the same time',
            'default': 'NO',
        },
    }

    _OPTIONAL_CONFIG = [
        'EVA_INPUT_WITH_HASH',
        'EVA_PRODUCTSTATUS_API_KEY',
        'EVA_PRODUCTSTATUS_USERNAME',
        'EVA_REFERENCE_TIME_THRESHOLD',
        'EVA_SINGLE_INSTANCE',
    ]

    _REQUIRED_CONFIG = [
        'executor',
    ]

    _PRODUCTSTATUS_REQUIRED_CONFIG = [
        'EVA_PRODUCTSTATUS_USERNAME',
        'EVA_PRODUCTSTATUS_API_KEY',
    ]

    PRODUCTSTATUS_REQUIRED_CONFIG = []

    PROCESS_PARTIAL_ONLY = 0
    PROCESS_PARTIAL_NO = 1
    PROCESS_PARTIAL_BOTH = 2

    def __init__(self):
        self.CONFIG.update(self._COMMON_ADAPTER_CONFIG)
        self.OPTIONAL_CONFIG = self.OPTIONAL_CONFIG + self._OPTIONAL_CONFIG
        self.REQUIRED_CONFIG = self.REQUIRED_CONFIG + self._REQUIRED_CONFIG

    def _factory(self):
        """!
        @brief Initialize the environment, then return this instance.
        """
        self._post_to_productstatus = None
        self._processing_failures = {}
        self.blacklist = set()
        self.required_uuids = set()
        self.reference_time_threshold_delta = None
        self.template = eva.template.Environment()
        return self

    def init(self):
        """!
        @param id an identifier for the adapter; must be constant across program restart
        @param api Productstatus API object
        @param environment_variables Dictionary of EVA_* environment variables
        """
        self.setup_process_partial()
        self.setup_single_instance()
        self.setup_reference_time_threshold()

        if self.post_to_productstatus():
            self.logger.info('Posting to Productstatus is ENABLED.')
        else:
            self.logger.warning('Posting to Productstatus is DISABLED due to insufficient configuration.')

    @property
    def executor(self):
        return self.env['executor']

    @property
    def api(self):
        return self.globe.productstatus

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

    def setup_single_instance(self):
        """!
        @brief Check that we have a Zookeeper endpoint if EVA requires that
        only a single instance is running at any given time.
        """
        if not self.env['EVA_SINGLE_INSTANCE']:
            return
        if not self.zookeeper:
            raise eva.exceptions.InvalidConfigurationException(
                'Running with EVA_SINGLE_INSTANCE enabled requires Zookeeper configuration.'
            )
        lock_path = os.path.join(self.zookeeper.EVA_BASE_PATH, 'single_instance_lock')
        try:
            self.logger.info('Creating a Zookeeper ephemeral node with path %s', lock_path)
            self.zookeeper.create(lock_path, None, ephemeral=True)
        except kazoo.exceptions.NodeExistsError:
            raise eva.exceptions.AlreadyRunningException('EVA is already running with the same group id, aborting!')

    def setup_reference_time_threshold(self):
        """!
        @brief Define the BaseAdapter.reference_time_threshold variable, which
        is either a datetime.timedelta object representing the difference from
        current time, or None. The variable is used to determine whether or not
        to process a specific dataset.
        """
        if self.env['EVA_REFERENCE_TIME_THRESHOLD'] != 0:
            self.reference_time_threshold_delta = datetime.timedelta(seconds=self.env['EVA_REFERENCE_TIME_THRESHOLD'])

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

    def set_processing_failures(self, uuid, failures):
        """!
        @brief Set the number of processing failures for the given UUID.
        @returns The number of failures registered.
        """
        if failures == 0:
            if uuid in self._processing_failures:
                del self._processing_failures[uuid]
        else:
            self._processing_failures[uuid] = failures
        self.statsd.gauge('consecutive_processing_failures', failures, {'event_id': str(uuid)})
        return failures

    def incr_processing_failures(self, uuid):
        """!
        @brief Increase the number of processing failures for the given UUID.
        @returns The number of failures registered.
        """
        failures = self.processing_failures(uuid) + 1
        return self.set_processing_failures(uuid, failures)

    def processing_failures(self, uuid):
        """!
        @brief Return the number of processing failures for the given UUID.
        """
        if uuid not in self._processing_failures:
            return 0
        return self._processing_failures[uuid]

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

    def reference_time_threshold(self):
        """!
        @brief Return a DateTime object which represent the oldest reference time that will be processed.
        """
        if self.reference_time_threshold_delta is not None:
            return eva.now_with_timezone() - self.reference_time_threshold_delta
        return eva.epoch_with_timezone()

    def resource_matches_input_config(self, resource):
        """!
        @brief Check that a Productstatus resource matches the configured
        processing criteria.
        """
        if resource._collection._resource_name != 'datainstance':
            self.logger.debug('Resource is not of type DataInstance, ignoring.')
        elif not self.in_array_or_empty(resource.data.productinstance.product.slug, 'EVA_INPUT_PRODUCT'):
            self.logger.debug('DataInstance belongs to Product "%s", ignoring.',
                              resource.data.productinstance.product.name)
        elif not self.in_array_or_empty(resource.servicebackend.slug, 'EVA_INPUT_SERVICE_BACKEND'):
            self.logger.debug('DataInstance is hosted on service backend %s, ignoring.',
                              resource.servicebackend.name)
        elif not self.in_array_or_empty(resource.format.slug, 'EVA_INPUT_DATA_FORMAT'):
            self.logger.debug('DataInstance file type is %s, ignoring.',
                              resource.format.name)
        elif not self.in_array_or_empty(resource.data.productinstance.reference_time.strftime('%H'), 'EVA_INPUT_REFERENCE_HOURS'):
            self.logger.debug('ProductInstance reference hour does not match any of %s, ignoring.', list(set(self.env['EVA_INPUT_REFERENCE_HOURS'])))
        elif self.reference_time_threshold() > resource.data.productinstance.reference_time:
            self.logger.debug('ProductInstance reference time is older than threshold of %s, ignoring.', self.reference_time_threshold())
        elif resource.deleted:
            self.logger.debug('DataInstance is marked as deleted, ignoring.')
        elif not self.resource_matches_hash_config(resource):
            self.logger.debug('DataInstance has hash and adapter is configured to not process instances with hashes, or vice versa. Ignoring.')
        elif resource.partial and self.process_partial == self.PROCESS_PARTIAL_NO and not productstatus.datainstance_has_complete_file_count(resource):
            self.logger.debug('DataInstance is not complete, ignoring.')
        elif resource.partial and self.process_partial == self.PROCESS_PARTIAL_ONLY and productstatus.datainstance_has_complete_file_count(resource):
            self.logger.debug('DataInstance is complete, ignoring.')
        elif self.is_blacklisted(resource.id):
            self.logger.debug('DataInstance %s is blacklisted, ignoring.', resource)
        elif self.is_blacklisted(resource.data.id):
            self.logger.debug('Data %s is blacklisted, ignoring.', resource.data)
        elif self.is_blacklisted(resource.data.productinstance.id):
            self.logger.debug('ProductInstance %s is blacklisted, ignoring.', resource.data.productinstance)
        elif not self.datainstance_has_required_uuids(resource):
            self.logger.debug('DataInstance %s does not have any relationships to required UUIDs %s, ignoring.', resource.data.productinstance, list(self.required_uuids))
        else:
            self.clear_required_uuids()
            self.logger.debug('DataInstance %s matches all configured criteria.', resource)
            return True

        return False

    def print_datainstance_info(self, datainstance, loglevel=logging.DEBUG):
        """!
        @brief Print information about a DataInstance to the debug log.
        """
        self.logger.log(loglevel,
                        'Product: %s [%s]',
                        datainstance.data.productinstance.product.name,
                        datainstance.data.productinstance.product.slug)
        self.logger.log(loglevel, 'ProductInstance: %s', datainstance.data.productinstance.id)
        self.logger.log(loglevel, 'Reference time: %s', eva.strftime_iso8601(datainstance.data.productinstance.reference_time, null_string=True))
        self.logger.log(loglevel, 'Time step: from %s to %s', eva.strftime_iso8601(datainstance.data.time_period_begin, null_string=True), eva.strftime_iso8601(datainstance.data.time_period_end, null_string=True))
        self.logger.log(loglevel, 'Data format: %s', datainstance.format.name)
        self.logger.log(loglevel, 'Service backend: %s', datainstance.servicebackend.name)

    def validate_resource(self, resource):
        """!
        @brief Check if the Resource fits this adapter, and send it to `process_resource`.
        @param resource A Productstatus resource.
        """
        print_info = bool(resource._collection._resource_name == 'datainstance')
        if not self.resource_matches_input_config(resource):
            if print_info:
                self.print_datainstance_info(resource, logging.DEBUG)
            return False
        self.print_datainstance_info(resource, logging.INFO)
        return True

    def create_job(self, message_id, resource):
        """!
        @brief Perform any action based on a Productstatus resource. The
        resource is guaranteed to be validated against input configuration.
        @param resource A Productstatus resource.
        """
        raise NotImplementedError()

    def finish_job(self, job):
        """!
        @brief After a task has been executed by the Executor, call this
        function in order to take action based on the data generated by the
        finished job.
        @param job A Job object.
        """
        raise NotImplementedError()

    def post_to_productstatus(self):
        """!
        @brief Returns True if this adapter has sufficient configuration to be
        able to post to Productstatus, False otherwise.
        """
        if self._post_to_productstatus is None:
            required_keys = self._PRODUCTSTATUS_REQUIRED_CONFIG + self.PRODUCTSTATUS_REQUIRED_CONFIG
            self._post_to_productstatus = True
            for key in required_keys:
                if not self.env[key]:
                    self._post_to_productstatus = False
                    break
        return self._post_to_productstatus

    def resource_matches_hash_config(self, resource):
        """!
        Returns true if one of the following criteria matches:

        * DataInstance.hash is NULL, and EVA_INPUT_WITH_HASH is set to NO
        * DataInstance.hash populated, and EVA_INPUT_WITH_HASH is set to YES
        * EVA_INPUT_WITH_HASH is unset
        """
        if self.env['EVA_INPUT_WITH_HASH'] is None:
            return True
        if resource.hash is None and self.env['EVA_INPUT_WITH_HASH'] is False:
            return True
        if resource.hash is not None and self.env['EVA_INPUT_WITH_HASH'] is True:
            return True
        return False

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

    def has_output_lifetime(self):
        """!
        @returns True if a DataInstance lifetime is specified, false otherwise.
        """
        return 'EVA_OUTPUT_LIFETIME' in self.env and self.env['EVA_OUTPUT_LIFETIME'] is not None

    def expiry_from_hours(self, hours):
        """!
        @returns a DateTime object representing an absolute DataInstance expiry
        time, calculated from the current time.
        """
        return eva.now_with_timezone() + datetime.timedelta(hours=hours)

    def expiry_from_lifetime(self):
        """!
        @returns a DateTime object representing an absolute DataInstance expiry
        time, based on the EVA_OUTPUT_LIFETIME environment variable. If the
        variable is not set, this function returns None.
        """
        if not self.has_output_lifetime():
            return None
        return self.expiry_from_hours(hours=self.env['EVA_OUTPUT_LIFETIME'])

    @staticmethod
    def default_resource_dictionary():
        """!
        @brief Returns a dictionary of resource types that will be populated with generate_resources().
        """
        return {
            'productinstance': [],
            'data': [],
            'datainstance': [],
        }

    def generate_and_post_resources(self, job):
        """!
        @brief Given a finished Job object, post information to Productstatus
        about newly created resources. Performs a number of sanity checks
        before posting any information.
        """
        if not self.post_to_productstatus():
            job.logger.warning('Skipping post to Productstatus because of missing configuration.')
            return

        if not job.complete():
            raise eva.exceptions.JobNotCompleteException('Refusing to post to Productstatus without a complete job.')

        try:
            job.logger.info('Generating Productstatus resources...')
            resources = self.default_resource_dictionary()
            self.generate_resources(job, resources)
        except ValueError:
            raise RuntimeError('generate_resources() did not return a tuple with three arrays, this is a bug in the code!')

        self.post_resources(job, resources)

        job.logger.info('Finished posting to Productstatus; all complete.')

    def generate_resources(self, job, resources):
        """!
        @brief Generate Productstatus resources based on finished job output.
        @param resources Dictionary with resource types that can be populated by the subclass implementation.
        """
        raise NotImplementedError('Please override this method in order to post to Productstatus.')

    def post_resources(self, job, resources):
        """!
        @brief Post information about a finished job to Productstatus. Takes a
        dictionary of arrays of Resource or EvaluatedResource objects.
        """
        job.logger.info('Saving %d resources to Productstatus.', sum([len(x) for x in resources.values()]))

        for resource_name in ['ProductInstance', 'Data', 'DataInstance']:
            resource_type = resource_name.lower()
            resource_list = resources[resource_type]

            for resource in resource_list:
                # lazy evaluation
                if isinstance(resource, productstatus.api.EvaluatedResource):
                    resource = resource.resource

                if resource.id is None:
                    job.logger.info('Creating new %s resource...', resource_name)
                else:
                    job.logger.info('Saving existing %s resource...', resource_name)

                # save the resource, with infinite retries
                eva.retry_n(resource.save,
                            exceptions=(productstatus.exceptions.ServiceUnavailableException,),
                            give_up=0,
                            logger=job.logger)

                # log the event
                job.logger.info('Saved %s resource: %s', resource_name, resource)
