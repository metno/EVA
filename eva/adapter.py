import logging

import eva.job
import eva.exceptions


class BaseAdapter(object):
    """
    Adapters contain all the information and configuration needed to translate
    a Productstatus event into job execution.
    """
    REQUIRED_CONFIG = {}

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

    def process_event(self, event, resource):
        """
        @brief Check if the Event and Resource fits this adapter, and execute any commands.
        @param event The message sent by the Productstatus server.
        @param resource The Productstatus resource referred to by the event.
        """
        raise NotImplementedError()

    def execute(self, job):
        """
        @brief Execute a job with the assigned Executor.
        """
        return self.executor.execute(job)

    def validate_configuration(self):
        """
        @brief Throw an exception if all required environment variables are not set.
        """
        for key, helptext in self.REQUIRED_CONFIG.iteritems():
            if key not in self.env:
                error = 'Missing required environment variable %s (%s)' % (key, helptext)
                raise eva.exceptions.MissingConfigurationException(error)


class NullAdapter(BaseAdapter):
    """
    An adapter that matches nothing and does nothing.
    """

    def process_event(self, *args, **kwargs):
        logging.info('NullAdapter has successfully sent the event to /dev/null')


class DownloadAdapter(BaseAdapter):
    """
    An adapter that downloads any posted DataInstance using wget.
    """
    REQUIRED_CONFIG = {
        'EVA_DOWNLOAD_DESTINATION': 'Where to download files',
    }

    def process_event(self, event, resource):
        if event.resource != 'datainstance':
            logging.info('Ignoring event with resource type %s', event.resource)
            return
        logging.info('Downloading data file %s...', resource.url)
        job = eva.job.Job()
        job.command = """
        set -ex
        echo "Running on host: `hostname`"
        echo "Working directory: `pwd`"
        cd %(destination)s
        echo "Productstatus DataInstance points to %(url)s"
        echo "Now downloading file..."
        wget %(url)s
        echo "Finished."
        """ % {
            'url': resource.url,
            'destination': self.env['EVA_DOWNLOAD_DESTINATION'],
        }
        self.execute(job)
