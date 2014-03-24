from __future__ import absolute_import
from __future__ import unicode_literals
from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements


class Requirements(SuiteRequirements):
    @property
    def self_referential_foreign_keys(self):
        return exclusions.closed()

    @property
    def index_reflection(self):
        return exclusions.closed()

    @property
    def view_reflection(self):
        # Hive supports views, but there's no SHOW VIEWS command, which breaks the tests.
        return exclusions.closed()

    @property
    def foreign_key_constraint_reflection(self):
        return exclusions.closed()

    @property
    def primary_key_constraint_reflection(self):
        return exclusions.closed()

    @property
    def unique_constraint_reflection(self):
        return exclusions.closed()

    @property
    def schemas(self):
        return exclusions.open()

    @property
    def date(self):
        # Added in Hive 0.12
        return exclusions.closed()

    @property
    def implements_get_lastrowid(self):
        return exclusions.closed()

    @property
    def views(self):
        return exclusions.closed()
