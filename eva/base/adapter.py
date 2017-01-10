"""
The Adapter is the single most important component of EVA. Adapters define how
to convert a message about created or changed metadata into meaningful work.
They are also responsible for creating metadata that should be posted back to
Productstatus.
"""


import datetime

import eva
import eva.config
import eva.globe
import eva.logger
import eva.job
import eva.exceptions

import productstatus
import productstatus.api


class BaseAdapter(eva.config.ConfigurableObject, eva.globe.GlobalMixin):
    """
    This is the adapter base class. All adapters must inherit this base class
    in order to function correctly.

    By deriving from this class, you may also use the following configuration
    variables. For a description of `types`, see the documentation for
    :class:`~eva.config.ConfigurableObject`.

    Variables marked as `required` **must** be configured, and `optional`
    **can** be configured. `explicit` variables are defined here for
    convenience, so that they can be re-used in subclasses with minimal code
    duplication.

    Variables starting with ``input_`` will be used to
    determine whether or not the adapter should process the incoming resource.

    .. table:: Base configuration for all adapters

       ===========================  ==============  ==============  ==========  ===========
       Variable                     Type            Default         Inclusion   Description
       ===========================  ==============  ==============  ==========  ===========
       concurrency                  |int|           1               optional    How many tasks that may run concurrently.
       executor                     |config_class|  (empty)         required    Which :class:`Executor <eva.base.executor.BaseExecutor>` to use.
       input_data_format            |list_string|   (empty)         optional    Only process resources with the specified data format(s).
       input_partial                |null_bool|     NO              optional    Only process resources that are either
                                                                                marked as partial (`YES`) or
                                                                                *not* marked as partial (`NO`).
                                                                                A null value means that the partial flag is ignored.
       input_product                |list_string|   (empty)         optional    Only process resources derived from the specified product(s).
       input_reference_hours        |list_int|      (empty)         optional    Only process resources where the ProductInstance reference hour
                                                                                matches any of the input values. An empty value will ignore
                                                                                the reference hour.
       input_service_backend        |list_string|   (empty)         optional    Only process resources with th e specified service backend(s).
       input_with_hash              |null_bool|     NULL            optional    Only process resources that either
                                                                                have a checksum hash stored (`YES`) or
                                                                                does *not* have a checksum hash stored (`NO`).
                                                                                A null value means that the hash is ignored.
       output_base_url              |string|        (empty)         explicit    The base URL for any output files generated.
                                                                                **THIS VARIABLE IS DEPRECATED.**
       output_data_format           |string|        (empty)         explicit    The data format for any output files generated.
       output_filename_pattern      |string|        (empty)         explicit    The filename pattern for any output files generated.
       output_lifetime              |int|           (empty)         explicit    Lifetime, in hours, for any output files generated.
                                                                                Defines how long the output files *must* live.
                                                                                A null value specifies that the output files must live forever.
       output_product               |string|        (empty)         explicit    The product name for any output files generated.
       output_service_backend       |string|        (empty)         explicit    The service backend for any output files generated.
       reference_time_threshold     |int|           0               optional    Never process resources whose reference time is older than N seconds.
                                                                                A value of zero specifies that the reference time is ignored.
       retry_backoff_factor         |float|         1.1             optional    Every time a job fails, its retry interval is multiplied by this number.
       retry_interval_secs          |int|           5               optional    How long to wait between re-scheduling failed jobs.
       retry_limit                  |int|           -1              optional    How many times to try re-scheduling a job if it fails. A negative number means to retry forever.
       ===========================  ==============  ==============  ==========  ===========
    """

    #! Common configuration variables all subclasses may use.
    _COMMON_ADAPTER_CONFIG = {
        'concurrency': {'type': 'int', 'default': '1', },
        'executor': {'type': 'config_class', 'default': '', },
        'input_data_format': {'type': 'list_string', 'default': '', },
        'input_partial': {'type': 'null_bool', 'default': 'NO', },
        'input_product': {'type': 'list_string', 'default': '', },
        'input_reference_hours': {'type': 'list_int', 'default': '', },
        'input_service_backend': {'type': 'list_string', 'default': '', },
        'input_with_hash': {'type': 'null_bool', 'default': '', },
        'output_base_url': {'type': 'string', 'default': '', },
        'output_data_format': {'type': 'string', 'default': '', },
        'output_filename_pattern': {'type': 'string', 'default': '', },
        'output_lifetime': {'type': 'int', 'default': '', },
        'output_product': {'type': 'string', 'default': '', },
        'output_service_backend': {'type': 'string', 'default': '', },
        'reference_time_threshold': {'type': 'int', 'default': '0', },
        'retry_backoff_factor': {'type': 'float', 'default': '1.1', },
        'retry_interval_secs': {'type': 'int', 'default': '5', },
        'retry_limit': {'type': 'int', 'default': '-1', },
    }

    _OPTIONAL_CONFIG = [
        'concurrency',
        'input_data_format',
        'input_partial',
        'input_product',
        'input_reference_hours',
        'input_service_backend',
        'input_with_hash',
        'reference_time_threshold',
        'retry_backoff_factor',
        'retry_interval_secs',
        'retry_limit',
    ]

    _REQUIRED_CONFIG = [
        'executor',
    ]

    PRODUCTSTATUS_REQUIRED_CONFIG = []

    PROCESS_PARTIAL_ONLY = 0
    PROCESS_PARTIAL_NO = 1
    PROCESS_PARTIAL_BOTH = 2

    def __init__(self):
        """
        Class initialization will merge common configuration options with
        subclass-defined configuration.
        """
        self.CONFIG.update(self._COMMON_ADAPTER_CONFIG)
        self.OPTIONAL_CONFIG = self.OPTIONAL_CONFIG + self._OPTIONAL_CONFIG
        self.REQUIRED_CONFIG = self.REQUIRED_CONFIG + self._REQUIRED_CONFIG

    def init(self):
        """
        Initialize base adapter functionality such as logging, blacklist,
        template environment, and so forth.
        """
        self.logger = self.create_logger(self.logger)

        self._post_to_productstatus = False
        self.blacklist = set()
        self.required_uuids = set()
        self.reference_time_threshold_delta = None
        self.template = eva.template.Environment()

        self._setup_process_partial()
        self._init_productstatus_output_resources()

        if self.env['reference_time_threshold'] != 0:
            self.reference_time_threshold_delta = datetime.timedelta(seconds=self.env['reference_time_threshold'])

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

    def _init_productstatus_output_resources(self):
        """
        Instantiate Productstatus resources referenced in output configuration.
        """
        self.output_data_format = self.api.dataformat[self.env['output_data_format']] \
            if self.isset('output_data_format') else None
        self.output_product = self.api.product[self.env['output_product']] \
            if self.isset('output_product') else None
        self.output_service_backend = self.api.servicebackend[self.env['output_service_backend']] \
            if self.isset('output_service_backend') else None

    @property
    def executor(self):
        """
        Returns the :class:`Executor <eva.base.executor.BaseExecutor>` instance for this adapter.
        """
        return self.env['executor']

    @property
    def api(self):
        """
        Returns the Productstatus API instance that is used by this adapter.
        """
        return self.globe.productstatus

    @property
    def concurrency(self):
        """
        Returns the number of jobs that this adapter should run concurrently.
        """
        return self.env['concurrency']

    def create_logger(self, logger):
        """
        Instantiates and returns a custom log adapter that will be used for log
        output by this adapter.

        :param logging.Logger logger: parent logging object.
        :rtype: logging.Logger
        :returns: a new log adapter.
        """
        return eva.logger.AdapterLogAdapter(logger, {'ADAPTER': self})

    def _setup_process_partial(self):
        """
        Set up the `process_partial` variable.
        """

        # FIXME: is this constant variable needed? Substitute for a function?
        if 'input_partial' not in self.env:
            self.process_partial = self.PROCESS_PARTIAL_NO
        elif self.env['input_partial'] is None:
            self.process_partial = self.PROCESS_PARTIAL_BOTH
        elif self.env['input_partial'] is True:
            self.process_partial = self.PROCESS_PARTIAL_ONLY
        else:
            self.process_partial = self.PROCESS_PARTIAL_NO

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

        elif not eva.in_array_or_empty(resource.data.productinstance.product.slug, self.env['input_product']):
            self.logger.debug("%s: belongs to Product '%s', ignoring.",
                              resource,
                              resource.data.productinstance.product.slug)

        elif not eva.in_array_or_empty(resource.servicebackend.slug, self.env['input_service_backend']):
            self.logger.debug("%s: hosted on service backend '%s', ignoring.",
                              resource,
                              resource.servicebackend.name)

        elif not eva.in_array_or_empty(resource.format.slug, self.env['input_data_format']):
            self.logger.debug("%s: file type is '%s', ignoring.",
                              resource,
                              resource.format.name)

        elif not eva.in_array_or_empty(resource.data.productinstance.reference_time.strftime('%H'), self.env['input_reference_hours']):
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
