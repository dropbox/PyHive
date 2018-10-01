# encoding: utf-8
from __future__ import absolute_import
from __future__ import unicode_literals
from pyhive import exc

import unittest


class TestError(unittest.TestCase):

    def test_str_error(self):
        """Test that an error created with a string is represented by the same string"""
        self.assertEqual(str(exc.Error("some error")), "some error")

    def test_dict_error(self):
        """Test that an error created with a dict is represented by the pretty formatted dict"""
        error = exc.Error({"a": {"b": "c"}})
        expected = """\
{
    "a": {
        "b": "c"
    }
}"""
        self.assertEqual(str(error), expected)
