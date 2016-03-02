import unittest
import logging

import eva
import eva.executor


class TestBase(unittest.TestCase):

    def test_in_array_or_empty(self):
        array = ['a', 'b', 'c']
        self.assertTrue(eva.in_array_or_empty('b', array))

    def test_in_array_or_empty_true_empty(self):
        array = []
        self.assertTrue(eva.in_array_or_empty('y', array))

    def test_in_array_or_empty_false(self):
        array = ['a', 'b', 'c']
        self.assertFalse(eva.in_array_or_empty('x', array))

    def test_url_to_filename(self):
        url = 'file:///foo/bar/baz.nc'
        filename = '/foo/bar/baz.nc'
        self.assertEqual(eva.url_to_filename(url), filename)

    def test_url_to_filename_wrong_protocol(self):
        with self.assertRaises(RuntimeError):
            eva.url_to_filename('https://example.com/foo.nc')

    def test_log_stdout_stderr(self):
        class Job(object):
            pass
        job = Job()
        job.id = 'foo'
        eva.executor.log_stdout_stderr(logging, job, ['x'], [])
