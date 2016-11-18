# coding: utf-8

import unittest
import logging

import eva.listener
import eva.tests


class TestRPCListener(eva.tests.TestBase):
    def test_init(self):
        listener = eva.listener.RPCListener()
        listener.set_globe(self.globe)
        listener.init()
