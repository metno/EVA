# coding: utf-8

import unittest
import logging

import eva.listener
import eva.globe
import eva.mail


class TestRPCListener(unittest.TestCase):
    def test_init(self):
        self.group_id = 'group-id'
        self.logger = logging
        self.mailer = eva.mail.NullMailer()
        self.statsd = eva.statsd.StatsDClient()
        self.zookeeper = None
        self.globe = eva.globe.Global(group_id=self.group_id,
                                      logger=self.logger,
                                      mailer=self.mailer,
                                      statsd=self.statsd,
                                      zookeeper=self.zookeeper,
                                      )
        eva.listener.RPCListener(self.globe, {})
