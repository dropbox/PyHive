# encoding: utf-8
from __future__ import absolute_import
from __future__ import unicode_literals
from pyhive import common
from past.builtins import unicode

import unittest


class ParamEscaperTestCase(unittest.TestCase):

    def setUp(self):
        self.escaper = common.ParamEscaper()

    def test_escape_args(self):
        self.assertEqual(self.escaper.escape_args({'foo': 'bar'}),
                         {'foo': "'bar'"})
        self.assertEqual(self.escaper.escape_args({'foo': 123}),
                         {'foo': 123})
        self.assertEqual(self.escaper.escape_args({'foo': 123.456}),
                         {'foo': 123.456})
        self.assertEqual(self.escaper.escape_args({'foo': ['a', 'b', 'c']}),
                         {'foo': "('a','b','c')"})
        self.assertEqual(self.escaper.escape_args({'foo': ('a', 'b', 'c')}),
                         {'foo': "('a','b','c')"})
        self.assertIn(self.escaper.escape_args({'foo': set(['a', 'b'])}),
                      ({'foo': "('a','b')"}, {'foo': "('b','a')"}))
        self.assertIn(self.escaper.escape_args({'foo': frozenset(['a', 'b'])}),
                      ({'foo': "('a','b')"}, {'foo': "('b','a')"}))

        self.assertEqual(self.escaper.escape_args(('bar',)),
                         ("'bar'",))
        self.assertEqual(self.escaper.escape_args([123]),
                         (123,))
        self.assertEqual(self.escaper.escape_args((123.456,)),
                         (123.456,))
        self.assertEqual(self.escaper.escape_args((['a', 'b', 'c'],)),
                         ("('a','b','c')",))
        self.assertEqual(self.escaper.escape_args(([unicode(u'你好'), 'b', 'c'],)),
                         ("('{}','b','c')".format(unicode(u'你好')),))
