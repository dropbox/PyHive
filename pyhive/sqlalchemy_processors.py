# Taken from SQLAlchemy with lots of stuff stripped out.

# sqlalchemy/processors.py
# Copyright (C) 2010-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
# Copyright (C) 2010 Gaetan de Menten gdementen@gmail.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""defines generic type conversion functions, as used in bind and result
processors.

They all share one common characteristic: None is passed through unchanged.

"""

import datetime
import re


def str_to_datetime_processor_factory(regexp, type_):
    rmatch = regexp.match
    # Even on python2.6 datetime.strptime is both slower than this code
    # and it does not support microseconds.
    has_named_groups = bool(regexp.groupindex)

    def process(value):
        if value is None:
            return None
        else:
            try:
                m = rmatch(value)
            except TypeError:
                raise ValueError("Couldn't parse %s string '%r' "
                                "- value is not a string." %
                                (type_.__name__, value))
            if m is None:
                raise ValueError("Couldn't parse %s string: "
                                "'%s'" % (type_.__name__, value))
            if has_named_groups:
                groups = m.groupdict(0)
                return type_(**dict(zip(groups.iterkeys(),
                                        map(int, groups.itervalues()))))
            else:
                return type_(*map(int, m.groups(0)))
    return process


DATETIME_RE = re.compile("(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?")

str_to_datetime = str_to_datetime_processor_factory(DATETIME_RE,
                                                    datetime.datetime)
