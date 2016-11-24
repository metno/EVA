import datetime
import logging

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
        'concurrency': {
            'type': 'int',
            'help': 'How many Executor tasks to run at the same time',
            'default': '1',
        },
        'executor': {
            'type': 'config_class',
            'help': 'Executor name from configuration files',
            'default': '',
        },
        'input_data_format': {
            'type': 'list_string',
            'help': 'Comma-separated input Productstatus data format slugs',
            'default': '',
        },
        'input_partial': {
            'type': 'null_bool',
            'help': 'Whether or not to process partial data instances',
            'default': 'NO',
        },
        'input_product': {
            'type': 'list_string',
            'help': 'Comma-separated input Productstatus product slugs',
            'default': '',
        },
        'input_service_backend': {
            'type': 'list_string',
            'help': 'Comma-separated input Productstatus service backend slugs',
            'default': '',
        },
        'input_reference_hours': {
            'type': 'list_int',
            'help': 'Comma-separated reference hours to process data for',
            'default': '',
        },
        'input_with_hash': {
            'type': 'null_bool',
            'help': 'Whether or not to process DataInstance resources containing a hash',
            'default': '',
        },
        'output_base_url': {
            'type': 'string',
            'help': 'Base URL for DataInstances posted to Productstatus',
            'default': '',
        },
        'output_data_format': {
            'type': 'string',
            'help': 'Productstatus Data Format ID for the finished product',
            'default': '',
        },
        'output_filename_pattern': {
            'type': 'string',
            'help': 'strftime pattern for output data instance filename',
            'default': '',
        },
        'output_lifetime': {
            'type': 'int',
            'help': 'Lifetime of output data instance, in hours, before it can be deleted',
            'default': '',
        },
        'output_product': {
            'type': 'string',
            'help': 'Productstatus Product ID for the finished product',
            'default': '',
        },
        'output_service_backend': {
            'type': 'string',
            'help': 'Productstatus Service Backend ID for the position of the finished product',
            'default': '',
        },
        'reference_time_threshold': {
            'type': 'int',
            'help': 'If non-zero, EVA will never process DataInstance resources that belong to a ProductInstance with a reference time older than N seconds.',
            'default': '0',
        },
    }

    _OPTIONAL_CONFIG = [
        'concurrency',
        'input_product',
        'input_with_hash',
        'reference_time_threshold',
    ]

    _REQUIRED_CONFIG = [
        'executor',
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
        self._post_to_productstatus = False
        self.blacklist = set()
        self.required_uuids = set()
        self.reference_time_threshold_delta = None
        self.template = eva.template.Environment()
        return self

    def init(self):
        """!
        @param id an identifier for the adapter; must be constant across program restart
        @param api Productstatus API object
        @param environment_variables Dictionary of * environment variables
        """
        self.logger = self.create_logger(self.logger)
        self.setup_process_partial()
        self.setup_reference_time_threshold()

        if self.post_to_productstatus():
            self.logger.info('Posting to Productstatus is ENABLED.')
        else:
            self.logger.warning('Posting to Productstatus is DISABLED due to insufficient configuration.')

        self.adapter_init()

    def adapter_init(self):
        """
        Provides a place for subclasses to initialize themselves. By default,
        this function does nothing.
        """
        pass

    @property
    def executor(self):
        return self.env['executor']

    @property
    def api(self):
        return self.globe.productstatus

    @property
    def concurrency(self):
        return self.env['concurrency']

    def create_logger(self, logger):
        """!
        @brief Returns a custom log adapter for logging contextual information
        about jobs.
        """
        return eva.logger.AdapterLogAdapter(logger, {'ADAPTER': self})

    def setup_process_partial(self):
        """!
        @brief Set up the `process_partial` variable.
        """
        if 'input_partial' not in self.env:
            self.process_partial = self.PROCESS_PARTIAL_NO
        elif self.env['input_partial'] is None:
            self.process_partial = self.PROCESS_PARTIAL_BOTH
        elif self.env['input_partial'] is True:
            self.process_partial = self.PROCESS_PARTIAL_ONLY
        else:
            self.process_partial = self.PROCESS_PARTIAL_NO

    def setup_reference_time_threshold(self):
        """!
        @brief Define the BaseAdapter.reference_time_threshold variable, which
        is either a datetime.timedelta object representing the difference from
        current time, or None. The variable is used to determine whether or not
        to process a specific dataset.
        """
        if self.env['reference_time_threshold'] != 0:
            self.reference_time_threshold_delta = datetime.timedelta(seconds=self.env['reference_time_threshold'])

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
            self.logger.debug("%s: resource is not of type DataInstance, ignoring.", resource)

        elif not self.in_array_or_empty(resource.data.productinstance.product.slug, 'input_product'):
            self.logger.debug("%s: belongs to Product '%s', ignoring.",
                              resource,
                              resource.data.productinstance.product.slug)

        elif not self.in_array_or_empty(resource.servicebackend.slug, 'input_service_backend'):
            self.logger.debug("%s: hosted on service backend '%s', ignoring.",
                              resource,
                              resource.servicebackend.name)

        elif not self.in_array_or_empty(resource.format.slug, 'input_data_format'):
            self.logger.debug("%s: file type is '%s', ignoring.",
                              resource,
                              resource.format.name)

        elif not self.in_array_or_empty(resource.data.productinstance.reference_time.strftime('%H'), 'input_reference_hours'):
            self.logger.debug("%s: ProductInstance reference hour does not match any of %s, ignoring.",
                              resource,
                              list(set(self.env['input_reference_hours'])))

        elif self.reference_time_threshold() > resource.data.productinstance.reference_time:
            self.logger.debug("%s: ProductInstance reference time is older than threshold of %s, ignoring.",
                              resource,
                              self.reference_time_threshold())

        elif resource.deleted:
            self.logger.debug("%s: marked as deleted, ignoring.",
                              resource)

        elif not self.resource_matches_hash_config(resource):
            self.logger.debug("%s: resource has hash, and adapter is configured to not process instances with hashes, or vice versa; ignoring.",
                              resource)

        elif resource.partial and self.process_partial == self.PROCESS_PARTIAL_NO and not productstatus.datainstance_has_complete_file_count(resource):
            self.logger.debug("%s: resource is incomplete; ignoring.",
                              resource)

        elif resource.partial and self.process_partial == self.PROCESS_PARTIAL_ONLY and productstatus.datainstance_has_complete_file_count(resource):
            self.logger.debug("%s: resource is complete; ignoring.",
                              resource)

        elif self.is_blacklisted(resource.id):
            self.logger.debug("%s: resource is blacklisted, ignoring.",
                              resource)

        elif self.is_blacklisted(resource.data.id):
            self.logger.debug("%s: resource Data %s is blacklisted, ignoring.",
                              resource,
                              resource.data)

        elif self.is_blacklisted(resource.data.productinstance.id):
            self.logger.debug("%s: ProductInstance %s is blacklisted, ignoring.",
                              resource,
                              resource.data.productinstance)

        elif not self.datainstance_has_required_uuids(resource):
            self.logger.debug("%s: resource does not have any relationships to required UUIDs %s, ignoring.",
                              resource,
                              list(self.required_uuids))

        else:
            self.clear_required_uuids()
            self.logger.info("%s matches all configured criteria.", resource)
            return True

        return False

    def validate_resource(self, resource):
        """
        Check if the provided Resource matches this adapter's processing criteria.

        :param productstatus.api.Resource resource: a Productstatus resource instance.
        :rtype: bool
        """
        return self.resource_matches_input_config(resource)

    def create_job(self, job):
        """!
        @brief Perform any action based on a Productstatus resource. The
        resource is guaranteed to be validated against input configuration.
        @type job eva.job.Job
        @param job The Job object to be operated on.
        """
        raise NotImplementedError()

    def finish_job(self, job):
        """!
        @brief After a task has been executed by the Executor, call this
        function in order to take action based on the data generated by the
        finished job.
        @type job eva.job.Job
        @param job The Job object to be operated on.
        """
        raise NotImplementedError()

    def post_to_productstatus(self):
        """!
        @brief Returns True if this adapter has sufficient configuration to be
        able to post to Productstatus, False otherwise.
        """
        if self.productstatus.has_credentials():
            self._post_to_productstatus = True
            for key in self.PRODUCTSTATUS_REQUIRED_CONFIG:
                if not self.env[key]:
                    self._post_to_productstatus = False
                    break
        return self._post_to_productstatus

    def resource_matches_hash_config(self, resource):
        """!
        Returns true if one of the following criteria matches:

        * DataInstance.hash is NULL, and input_with_hash is set to NO
        * DataInstance.hash populated, and input_with_hash is set to YES
        * input_with_hash is unset
        """
        if self.env['input_with_hash'] is None:
            return True
        if resource.hash is None and self.env['input_with_hash'] is False:
            return True
        if resource.hash is not None and self.env['input_with_hash'] is True:
            return True
        return False

    def has_productstatus_credentials(self):
        """!
        @return True if the adapter is configured with a user name and API key
        to Productstatus, False otherwise.
        """
        return self.productstatus.has_credentials()

    def require_productstatus_credentials(self):
        """!
        @brief Raise an exception if Productstatus credentials are not configured.
        """
        raise RuntimeError('This function is obsolete, please remove this requirement.')

    def has_output_lifetime(self):
        """!
        @returns True if a DataInstance lifetime is specified, false otherwise.
        """
        return 'output_lifetime' in self.env and self.env['output_lifetime'] is not None

    def expiry_from_hours(self, hours):
        """!
        @returns a DateTime object representing an absolute DataInstance expiry
        time, calculated from the current time.
        """
        return eva.now_with_timezone() + datetime.timedelta(hours=hours)

    def expiry_from_lifetime(self):
        """!
        @returns a DateTime object representing an absolute DataInstance expiry
        time, based on the output_lifetime environment variable. If the
        variable is not set, this function returns None.
        """
        if not self.has_output_lifetime():
            return None
        return self.expiry_from_hours(hours=self.env['output_lifetime'])

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
