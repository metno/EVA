import unittest

import eva


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
