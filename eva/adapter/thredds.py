import os.path
import requests
import time

import eva
import eva.base.adapter
import eva.job
import eva.exceptions
import eva.template

import productstatus


class ThreddsAdapter(eva.base.adapter.BaseAdapter):
    """!
    An adapter that listens to events about files being produced on /lustre
    and then polls the Thredds server until a matching dataset is available
    there, then it posts an information about it to productstatus.
    """

    CONFIG = {
        'EVA_THREDDS_POLL_INTERVAL': {
            'type': 'int',
            'help': 'How often should Thredds server be checked.',
            'default': '20',
        },
        'EVA_THREDDS_POLL_RETRIES': {
            'type': 'int',
            'help': 'Number of times to check for the data on Thredds server.',
            'default': '6',
        },
        'EVA_THREDDS_BASE_URL': {
            'type': 'string',
            'help': 'Base URL to prepend to the filename from the input resource',
            'default': 'http://thredds.met.no/thredds/dodsC/radarnowcasting/pp/',
        },
    }

    REQUIRED_CONFIG = [
        'EVA_INPUT_PRODUCT',
    ]

    OPTIONAL_CONFIG = [
        'EVA_THREDDS_POLL_INTERVAL',
        'EVA_THREDDS_POLL_RETRIES',
        'EVA_THREDDS_BASE_URL',
    ]

    def init(self):
        """!
        @brief Check that optional configuration is consistent.
        """
        if self.has_valid_output_config():
            self.post_to_productstatus = True
            self.require_productstatus_credentials()
        else:
            self.post_to_productstatus = False
            self.logger.warning('Will not post any data to Productstatus.')

        self.eva_input_product = self.env['EVA_INPUT_PRODUCT']
        self.thredds_poll_interval = self.env['EVA_THREDDS_POLL_INTERVAL']
        self.thredds_poll_retries = self.env['EVA_THREDDS_POLL_RETRIES']
        self.thredds_base_url = self.env['EVA_THREDDS_BASE_URL']

    def has_valid_output_config(self):
        """!
        @return True if all optional output variables are configured, False otherwise.
        """
        return (
            (self.env['EVA_INPUT_PRODUCT'] is not None)
        )

    def process_resource(self, message_id, resource):
        """!
        @brief Check if the resource is reachable via the provided URL if not, sleep and try again
        """
        url = resource.url
        basename = os.path.basename(url)

        thredds_url = "{}{}.html".format(self.thredds_base_url, basename)

        self.logger.info("Will check up to {} times if {} is reachable.".format(self.thredds_poll_retries, thredds_url))
        for x in range(self.thredds_poll_retries):
            self.logger.info("Trial {}/{}".format(x + 1, self.thredds_poll_retries))
            try:
                r = requests.head(thredds_url)
                if r.status_code == requests.codes.ok:
                    self.logger.info("The data is reachable at {0}". format(thredds_url))
                    if self.post_to_productstatus:
                        self.add_datainstance_to_productstatus(resource, thredds_url)
                    return
                else:
                    self.logger.info("The data is not available at {0}, sleeping for {1} seconds...".format(
                                    thredds_url, self.thredds_poll_interval))
                    time.sleep(self.thredds_poll_interval)
            except requests.exceptions.RequestException as e:
                self.logger.info("Problem with connection while trying to access {}, sleeping for {} seconds...".format(
                                thredds_url, self.thredds_poll_interval))
                self.logger.info("Error: {}".format(e))
                time.sleep(self.thredds_poll_interval)
                continue

    def add_datainstance_to_productstatus(self, resource, threddsurl):
        self.logger.info("Creating a new DataInstance on the Productstatus server...")

        parameters = {
            'data': resource.data,
            'format': resource.format,
            'servicebackend': self.api.servicebackend['thredds'],
            'url': threddsurl,
        }

        datainstance = self.api.datainstance.find_or_create(parameters)
        datainstance.expires = resource.expires
        eva.retry_n(datainstance.save,
                    exceptions=(productstatus.exceptions.ServiceUnavailableException,),
                    give_up=0)
        self.logger.info("DataInstance {}, expires {}".format(datainstance, datainstance.expires))
