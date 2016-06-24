import unittest
import logging

import productstatus
import productstatus.api
import productstatus.event

import eva.adapter
import eva.executor


BLANK_UUID = '00000000-0000-0000-0000-000000000000'
RANDOM_UUID = 'f194279e-dfa8-45ff-ab62-1b03d89e9705'


class TestThreddsAdapter(unittest.TestCase):

    def setUp(self):
        self.env = {
            'EVA_INPUT_DATA_FORMAT': BLANK_UUID,
            'EVA_INPUT_PRODUCT': BLANK_UUID,
            'EVA_INPUT_SERVICE_BACKEND': BLANK_UUID,
        }
        self.productstatus_api = productstatus.api.Api('http://localhost:8000')
        self.logger = logging
        self.zookeeper = None
        self.executor = eva.executor.NullExecutor(None, self.env, self.logger, self.zookeeper)

    def create_adapter(self):
        self.adapter = eva.adapter.ThreddsAdapter(self.env, self.executor, self.productstatus_api, self.logger, self.zookeeper)

    def test_create(self):
        """!
        @brief Test that ThreddsAdapter can be instantiated.
        """
        self.create_adapter()
