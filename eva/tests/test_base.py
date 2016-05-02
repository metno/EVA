# coding: utf-8

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
        job = eva.job.Job('foo', logging.getLogger('root'))
        eva.executor.log_stdout_stderr(job, ['x'], [])

    def test_split_comma_separated(self):
        string = ' foo , bar,baz  '
        list_ = eva.split_comma_separated(string)
        self.assertListEqual(list_, ['foo', 'bar', 'baz'])

    def test_parse_boolean_string_true(self):
        self.assertTrue(eva.parse_boolean_string('yes'))
        self.assertTrue(eva.parse_boolean_string('YES'))
        self.assertTrue(eva.parse_boolean_string('true'))
        self.assertTrue(eva.parse_boolean_string('TRUE'))
        self.assertTrue(eva.parse_boolean_string('True'))
        self.assertTrue(eva.parse_boolean_string('1'))

    def test_parse_boolean_string_false(self):
        self.assertFalse(eva.parse_boolean_string('no'))
        self.assertFalse(eva.parse_boolean_string('NO'))
        self.assertFalse(eva.parse_boolean_string('false'))
        self.assertFalse(eva.parse_boolean_string('FALSE'))
        self.assertFalse(eva.parse_boolean_string('False'))
        self.assertFalse(eva.parse_boolean_string('0'))

    def test_zookeeper_group_id(self):
        self.assertEqual(eva.zookeeper_group_id(u'/this/~isaán/\000ID'), b'this.~isan..id')
        with self.assertRaises(eva.exceptions.InvalidGroupIdException):
            eva.zookeeper_group_id(u'áćé')
        with self.assertRaises(eva.exceptions.InvalidGroupIdException):
            eva.zookeeper_group_id('zookeeper')
