import unittest
import logging
import copy
import uuid
import mock
import httmock

import productstatus
import productstatus.api

import eva.globe
import eva.mail
import eva.base.adapter
import eva.executor
import eva.statsd
import eva.tests.schemas


class TestBase(unittest.TestCase):
    def setUp(self):
        self.group_id = 'group_id'
        self.logger = logging.getLogger('root')
        self.zookeeper = mock.MagicMock()
        self.statsd = mock.MagicMock()
        self.mailer = mock.MagicMock()
        self.productstatus = mock.MagicMock()
        self.setup_globe()

    def setup_globe(self):
        self.globe = eva.globe.Global(group_id=self.group_id,
                                      logger=self.logger,
                                      mailer=self.mailer,
                                      productstatus=self.productstatus,
                                      statsd=self.statsd,
                                      zookeeper=self.zookeeper,
                                      )


class BaseTestAdapter(unittest.TestCase):
    """!
    @brief Base class for adapter tests.
    """

    adapter_class = eva.base.adapter.BaseAdapter
    environment = {}

    def setUp(self):
        self.env = copy.copy(self.environment)
        self.group_id = 'group-id'
        self.productstatus_api = productstatus.api.Api('http://localhost:8000')
        self.logger = logging.getLogger('root')
        self.zookeeper = None
        self.mailer = eva.mail.NullMailer()
        self.statsd = eva.statsd.StatsDClient()
        self.globe = eva.globe.Global(group_id=self.group_id,
                                      logger=self.logger,
                                      mailer=self.mailer,
                                      statsd=self.statsd,
                                      zookeeper=self.zookeeper,
                                      )
        self.executor = eva.executor.NullExecutor(None, self.env, self.globe)

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
            self.adapter = self.adapter_class(self.env, self.executor, self.productstatus_api, self.globe)
