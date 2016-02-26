import logging
import os
import datetime
import re
import uuid

import eva
import eva.job
import eva.exceptions

import productstatus


class BaseAdapter(eva.ConfigurableObject):
    """
    Adapters contain all the information and configuration needed to translate
    a Productstatus resource into job execution.
    """

    # @brief Common configuration variables all subclasses may use.
    _COMMON_ADAPTER_CONFIG = {
        'EVA_INPUT_DATA_FORMAT_UUID': 'Comma-separated input Productstatus data format UUIDs',
        'EVA_INPUT_PRODUCT_UUID': 'Comma-separated input Productstatus product UUIDs',
        'EVA_INPUT_SERVICE_BACKEND_UUID': 'Comma-separated input Productstatus service backend UUIDs',
        'EVA_OUTPUT_BASE_URL': 'Base URL for DataInstances posted to Productstatus',
        'EVA_OUTPUT_FILENAME_PATTERN': 'strftime pattern for output data instance filename',
        'EVA_OUTPUT_LIFETIME': 'Lifetime of output data instance, in hours, before it can be deleted',
        'EVA_OUTPUT_PRODUCT_UUID': 'Productstatus Product UUID for the finished product',
        'EVA_OUTPUT_SERVICE_BACKEND_UUID': 'Productstatus Service Backend UUID for the position of the finished product',
    }

    def __init__(self, environment_variables, executor, api):
        """
        @param id an identifier for the adapter; must be constant across program restart
        @param api Productstatus API object
        @param environment_variables Dictionary of EVA_* environment variables
        """
        self.CONFIG.update(self._COMMON_ADAPTER_CONFIG)
        self.executor = executor
        self.api = api
        self.env = environment_variables
        self.normalize_config_uuids()
        self.validate_configuration()
        self.init()

    def normalize_config_uuids(self):
        """
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
            if uuid_input_regex.match(key):
                self.env[key] = [x.strip() for x in self.env[key].strip().split(',')]
                uuids = self.env[key]
            elif uuid_regex.match(key):
                uuids = [self.env[key]]
            else:
                continue
            for id in uuids:
                try:
                    uuid.UUID(id)
                except ValueError, e:
                    logging.critical("Invalid UUID '%s' in configuration variable %s: %s" % (id, key, e))
                    errors += 1
        if errors > 0:
            raise eva.exceptions.InvalidConfigurationException('%d errors occurred during UUID normalization' % errors)

    def in_array_or_empty(self, id, array):
        """
        @returns true if `id` is found in `array`, or `array` is empty.
        """
        return (len(array) == 0) or (id in array)

    def resource_matches_input_config(self, resource):
        """
        @brief Check that a Productstatus resource matches the configured
        processing criteria.
        """
        if resource._collection._resource_name != 'datainstance':
            logging.debug('Resource is not of type DataInstance, ignoring.')

        elif self.in_array_or_empty(resource.data.productinstance.product.id, self.env['EVA_INPUT_PRODUCT_UUID']):
            logging.debug('DataInstance belongs to Product "%s", ignoring.',
                          resource.data.productinstance.product.name)

        elif self.in_array_or_empty(resource.servicebackend.id, self.env['EVA_INPUT_SERVICE_BACKEND_UUID']):
            logging.debug('DataInstance is hosted on service backend %s, ignoring.',
                          resource.servicebackend.name)

        elif self.in_array_or_empty(resource.format.id, self.env['EVA_INPUT_DATA_FORMAT_UUID']):
            logging.debug('DataInstance file type is %s, ignoring.',
                          resource.format.name)
        else:
            logging.debug('DataInstance matches all configured criteria, will download from %s...', resource.url)
            return True

        return False

    def validate_and_process_resource(self, resource):
        """
        @brief Check if the Resource fits this adapter, and send it to `process_resource`.
        @param resource A Productstatus resource.
        """
        if not self.resource_matches_input_config(resource):
            return
        logging.info('Start processing resource: %s', resource)
        self.process_resource(resource)
        logging.info('Finish processing resource: %s', resource)

    def process_resource(self, resource):
        """
        @brief Perform any action based on a Productstatus resource. The
        resource is guaranteed to be validated against input configuration.
        @param resource A Productstatus resource.
        """
        raise NotImplementedError()

    def init(self):
        """
        @brief This function provides a place for subclasses to initialize itself before accepting jobs.
        """
        pass

    def execute(self, job):
        """
        @brief Execute a job with the assigned Executor.
        """
        return self.executor.execute(job)

    def has_productstatus_credentials(self):
        """
        @return True if the adapter is configured with a user name and API key
        to Productstatus, False otherwise.
        """
        return (
            ('EVA_PRODUCTSTATUS_USERNAME' in self.env and self.env['EVA_PRODUCTSTATUS_USERNAME'] is not None) and
            ('EVA_PRODUCTSTATUS_API_KEY' in self.env and self.env['EVA_PRODUCTSTATUS_API_KEY'] is not None)
        )

    def require_productstatus_credentials(self):
        """
        @brief Raise an exception if Productstatus credentials are not configured.
        """
        if not self.has_productstatus_credentials():
            raise eva.exceptions.MissingConfigurationException(
                'Posting to Productstatus requires environment variables EVA_PRODUCTSTATUS_USERNAME and EVA_PRODUCTSTATUS_API_KEY.'
            )


class NullAdapter(BaseAdapter):
    """
    An adapter that matches nothing and does nothing.
    """

    def process_resource(self, *args, **kwargs):
        logging.info('NullAdapter has successfully sent the resource to /dev/null')


class TestExecutorAdapter(BaseAdapter):
    """
    An adapter that echoes the URL of the received DataInstance.
    """

    def process_resource(self, resource):
        """
        @brief Execute a Job that echoes the URI of the received resource.
        """
        job = eva.job.Job()
        job.command = """#!/bin/bash
        echo %(url)s
        """ % {
            'url': resource.url,
        }
        self.execute(job)


class DownloadAdapter(BaseAdapter):
    """
    An adapter that downloads files, and optionally posts their metadata to Productstatus.

    The consumer specifies Productstatus product, format, and service backend.
    The adapter triggers on DataInstance events, downloading matching files
    using wget.

    If the file is successfully downloaded, and the adapter has been configured
    with Productstatus credentials and the EVA_OUTPUT_* optional configuration,
    the file will be registered as a new DataInstance in Productstatus.
    """

    CONFIG = {
        'EVA_DOWNLOAD_DESTINATION': 'Where to place downloaded files',
    }

    REQUIRED_CONFIG = [
        'EVA_DOWNLOAD_DESTINATION',
        'EVA_INPUT_DATA_FORMAT_UUID',
        'EVA_INPUT_PRODUCT_UUID',
        'EVA_INPUT_SERVICE_BACKEND_UUID',
    ]

    OPTIONAL_CONFIG = [
        'EVA_OUTPUT_BASE_URL',
        'EVA_OUTPUT_LIFETIME',
        'EVA_OUTPUT_SERVICE_BACKEND_UUID',
    ]

    def init(self):
        """
        @brief Check that optional configuration is consistent.
        """
        if self.has_valid_output_config():
            self.post_to_productstatus = True
            self.require_productstatus_credentials()
            self.lifetime = datetime.timedelta(hours=int(self.env['EVA_OUTPUT_LIFETIME']))
            if self.env['EVA_INPUT_SERVICE_BACKEND_UUID'] == self.env['EVA_OUTPUT_SERVICE_BACKEND_UUID']:
                raise eva.exceptions.InvalidConfigurationException('EVA_INPUT_SERVICE_BACKEND_UUID and EVA_OUTPUT_SERVICE_BACKEND_UUID cannot be equal as that will result in an endless loop.')
        else:
            self.post_to_productstatus = False
            logging.debug('Will not post any data to Productstatus.')

    def has_valid_output_config(self):
        """
        @return True if all optional output variables are configured, False otherwise.
        """
        return (
            (self.env['EVA_OUTPUT_BASE_URL'] is not None) and
            (self.env['EVA_OUTPUT_LIFETIME'] is not None) and
            (self.env['EVA_OUTPUT_SERVICE_BACKEND_UUID'] is not None)
        )

    def process_resource(self, resource):
        """
        @brief Download a file, and optionally post the result to Productstatus.
        """
        filename = os.path.basename(resource.url)
        job = eva.job.Job()
        job.command = """#!/bin/bash
        wget --no-verbose --output-document='%(destination)s' %(url)s
        """ % {
            'url': resource.url,
            'destination': os.path.join(self.env['EVA_DOWNLOAD_DESTINATION'], filename)
        }
        self.execute(job)

        if job.status != eva.job.COMPLETE:
            raise eva.exceptions.RetryException("Download of '%s' failed." % resource.url)

        if not self.post_to_productstatus:
            return

        logging.debug('Creating a new DataInstance on the Productstatus server...')
        datainstance = self.api.datainstance.create()
        datainstance.data = resource.data
        datainstance.format = resource.format
        datainstance.expires = datetime.datetime.now() + self.lifetime
        datainstance.servicebackend = self.api.servicebackend[self.env['EVA_OUTPUT_SERVICE_BACKEND_UUID']]
        datainstance.url = os.path.join(self.env['EVA_OUTPUT_BASE_URL'], filename)
        eva.retry_n(datainstance.save,
                    exceptions=(productstatus.exceptions.ServiceUnavailableException,),
                    give_up=0)
        logging.info('DataInstance %s, expires %s', datainstance, datainstance.expires)
