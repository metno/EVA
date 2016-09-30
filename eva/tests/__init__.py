import unittest
import logging
import copy
import uuid
import httmock

import productstatus
import productstatus.api

import eva.base.adapter
import eva.executor
import eva.statsd
import eva.tests.schemas


class BaseTestAdapter(unittest.TestCase):
    """!
    @brief Base class for adapter tests.
    """

    adapter_class = eva.base.adapter.BaseAdapter
    environment = {}

    def setUp(self):
        self.env = copy.copy(self.environment)
        self.productstatus_api = productstatus.api.Api('http://localhost:8000')
        self.logger = logging.getLogger('root')
        self.zookeeper = None
        self.statsd = eva.statsd.StatsDClient()
        self.executor = eva.executor.NullExecutor(None, self.env, self.logger, self.zookeeper, self.statsd)

    def random_uuid(self):
        return str(uuid.uuid4())

    def create_job(self, resource):
        job = self.adapter.create_job(self.random_uuid(), resource)
        job.resource = resource
        return job

    def generate_resources(self, job):
        resources = eva.base.adapter.BaseAdapter.default_resource_dictionary()
        self.adapter.generate_resources(job, resources)
        return resources

    def create_adapter(self):
        with httmock.HTTMock(*eva.tests.schemas.SCHEMAS):
            self.adapter = self.adapter_class(self.env, self.executor, self.productstatus_api, self.logger, self.zookeeper, self.statsd)
