# coding: utf-8

import unittest
import uuid

import eva.executor.grid_engine


BLANK_UUID = '00000000-0000-0000-0000-000000000000'


class TestGridEngineExecutor(unittest.TestCase):
    def test_create_job_unique_id(self):
        compare = 'eva.y-h-gr34t--job.%s' % BLANK_UUID
        group_id = u'/yæh/gr34t/~job~/'
        id = eva.executor.grid_engine.create_job_unique_id(group_id, BLANK_UUID)
        self.assertEqual(id, compare)
