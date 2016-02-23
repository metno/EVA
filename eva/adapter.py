import logging
import os
import datetime

import eva
import eva.job
import eva.exceptions

import productstatus


class BaseAdapter(eva.ConfigurableObject):
    """
    Adapters contain all the information and configuration needed to translate
    a Productstatus resource into job execution.
    """

    def __init__(self, environment_variables, executor, api):
        """
        @param id an identifier for the adapter; must be constant across program restart
        @param api Productstatus API object
        @param environment_variables Dictionary of EVA_* environment variables
        """
        self.executor = executor
        self.api = api
        self.env = environment_variables
        self.validate_configuration()
        self.init()

    def process_resource(self, resource):
        """
        @brief Check if the Resource fits this adapter, and execute any
        commands. This function must be overridden by subclasses.
        @param resource The Productstatus resource referred to by the event.
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
            (self.env['EVA_PRODUCTSTATUS_USERNAME'] is not None) and
            (self.env['EVA_PRODUCTSTATUS_API_KEY'] is not None)
        )


class NullAdapter(BaseAdapter):
    """
    An adapter that matches nothing and does nothing.
    """

    def process_resource(self, *args, **kwargs):
        logging.info('NullAdapter has successfully sent the resource to /dev/null')


class TestExecutorAdapter(BaseAdapter):
    """
    An adapter that matches nothing and does nothing.
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
    with Productstatus credentials and the EVA_DOWNLOAD_OUTPUT_* optional
    configuration, the file will be registered as a new DataInstance in Productstatus.
    """

    REQUIRED_CONFIG = {
        'EVA_DOWNLOAD_PRODUCT_UUID': 'Comma-separated Productstatus Product UUIDs to download files for',
        'EVA_DOWNLOAD_FORMAT_UUID': 'Comma-separated Productstatus formats to filter by',
        'EVA_DOWNLOAD_INPUT_DESTINATION': 'Where to place downloaded files',
        'EVA_DOWNLOAD_INPUT_SERVICE_BACKEND_UUID': 'Comma-separated Productstatus Service Backend UUIDs to download from',
    }

    OPTIONAL_CONFIG = {
        'EVA_DOWNLOAD_OUTPUT_BASE_URL': 'Base URL for DataInstances posted to Productstatus',
        'EVA_DOWNLOAD_OUTPUT_LIFETIME': 'Lifetime of downloaded dataset, in hours, before it will be deleted',
        'EVA_DOWNLOAD_OUTPUT_SERVICE_BACKEND_UUID': 'If set, registers a Productstatus DataInstance with the given Service Backend UUID',
    }

    def init(self):
        """
        @brief Normalize comma-separated environment variables, and check that
        optional configuration is consistent.
        """
        for key in ['EVA_DOWNLOAD_PRODUCT_UUID',
                    'EVA_DOWNLOAD_FORMAT_UUID',
                    'EVA_DOWNLOAD_INPUT_SERVICE_BACKEND_UUID',
                    ]:
            self.env[key] = [x.strip() for x in self.env[key].strip().split(',')]

        # Require valid configuration combination
        if self.has_valid_output_config():
            self.post_to_productstatus = True
            self.lifetime = datetime.timedelta(hours=int(self.env['EVA_DOWNLOAD_OUTPUT_LIFETIME']))
            if not self.has_productstatus_credentials():
                raise eva.exceptions.MissingConfigurationException(
                    'Posting to Productstatus requires environment variables EVA_PRODUCTSTATUS_USERNAME and EVA_PRODUCTSTATUS_API_KEY.'
                )
            if self.env['EVA_DOWNLOAD_INPUT_SERVICE_BACKEND_UUID'] == self.env['EVA_DOWNLOAD_OUTPUT_SERVICE_BACKEND_UUID']:
                raise eva.exceptions.InvalidConfigurationException('EVA_DOWNLOAD_INPUT_SERVICE_BACKEND_UUID and EVA_DOWNLOAD_OUTPUT_SERVICE_BACKEND_UUID cannot be equal as that will result in an endless loop.')
        else:
            self.post_to_productstatus = False
            logging.debug('Will not post any data to Productstatus.')

    def has_valid_output_config(self):
        """
        @return True if all optional output variables are configured, False otherwise.
        """
        return (
            (self.env['EVA_DOWNLOAD_OUTPUT_SERVICE_BACKEND_UUID'] is not None) and
            (self.env['EVA_DOWNLOAD_OUTPUT_LIFETIME'] is not None) and
            (self.env['EVA_DOWNLOAD_OUTPUT_BASE_URL'] is not None)
        )

    def resource_matches(self, resource):
        """
        @brief Check that the resource matches the configured processing criteria.
        """
        if resource._collection._resource_name != 'datainstance':
            logging.debug('Resource is not of type DataInstance, ignoring.')
        elif resource.data.productinstance.product.id not in self.env['EVA_DOWNLOAD_PRODUCT_UUID']:
            logging.debug('DataInstance belongs to Product "%s", ignoring.',
                         resource.data.productinstance.product.name)
        elif resource.servicebackend.id not in self.env['EVA_DOWNLOAD_INPUT_SERVICE_BACKEND_UUID']:
            logging.debug('DataInstance is hosted on service backend %s, ignoring.',
                         resource.servicebackend.name)
        elif resource.format.id not in self.env['EVA_DOWNLOAD_FORMAT_UUID']:
            logging.debug('DataInstance file type is %s, ignoring.',
                         resource.format.name)
        else:
            return True
        return False

    def process_resource(self, resource):
        """
        @brief Check if the file matches, download it, and post to Productstatus.
        """
        if not self.resource_matches(resource):
            return
        logging.info('DataInstance matches configured criteria, will download from %s...', resource.url)

        filename = os.path.basename(resource.url)
        job = eva.job.Job()
        job.command = """#!/bin/bash
        wget --no-verbose --output-document='%(destination)s' %(url)s
        """ % {
            'url': resource.url,
            'destination': os.path.join(self.env['EVA_DOWNLOAD_INPUT_DESTINATION'], filename)
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
        datainstance.servicebackend = self.api.servicebackend[self.env['EVA_DOWNLOAD_OUTPUT_SERVICE_BACKEND_UUID']]
        datainstance.url = os.path.join(self.env['EVA_DOWNLOAD_OUTPUT_BASE_URL'], filename)
        eva.retry_n(datainstance.save,
                    exceptions=(productstatus.exceptions.ServiceUnavailableException,),
                    give_up=0)
        logging.info('DataInstance %s, expires %s', datainstance, datainstance.expires)
