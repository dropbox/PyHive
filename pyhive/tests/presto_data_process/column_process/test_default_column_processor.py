from unittest import TestCase
from pyhive.presto_data_process.column_process.default_cell_processor import \
    PrestoDefaultCellProcessor


class TestDefaultColumnProcessor(TestCase):
    def test_given_some_raw_cell_when_process_cell_should_return_the_raw_cell(self):
        self.assertEqual(2, PrestoDefaultCellProcessor().process_raw_cell(2))
