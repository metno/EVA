# coding: utf-8

import unittest
import logging

import eva.listener


class TestRPCListener(unittest.TestCase):
    def test_init(self):
        eva.listener.RPCListener({}, logging, None)
