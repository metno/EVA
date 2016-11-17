import configparser
import copy
import httmock
import logging
import mock
import unittest
import uuid

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


class BaseTestAdapter(TestBase):
    """!
    @brief Base class for adapter tests.
    """

    adapter_class = eva.base.adapter.BaseAdapter
    base_config_ini = \
"""
[defaults.adapter]
executor = foo
"""
    config_ini = ""

    def setUp(self):
        super().setUp()
        self.executor = eva.executor.NullExecutor()
        self.executor.set_globe(self.globe)
        self.config = configparser.ConfigParser()
        self.config.read_string(self.base_config_ini)
        self.config.read_string(self.config_ini)
        assert 'adapter' in self.config.sections()

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

    def create_adapter(self, key='adapter'):
        incubator, self.adapter = self.adapter_class().factory(self.config, 'defaults.adapter', key)
        self.adapter.set_globe(self.globe)
        self.adapter.init()
