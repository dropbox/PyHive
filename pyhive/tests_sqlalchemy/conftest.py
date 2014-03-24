from __future__ import absolute_import
from __future__ import unicode_literals
from distutils.version import StrictVersion
import sqlalchemy

if StrictVersion(sqlalchemy.__version__) >= StrictVersion('0.9.4'):
    from sqlalchemy.dialects import registry

    registry.register("hive", "pyhive.sqlalchemy_hive", "HiveDialect")
    registry.register("presto", "pyhive.sqlalchemy_presto", "PrestoDialect")

    from sqlalchemy.testing.plugin.pytestplugin import *
