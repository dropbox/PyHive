from unittest import TestCase
from mock import MagicMock
from pyhive.exc import DataError
from pyhive.presto_data_process.cell_processor import PrestoCellProcessor
from pyhive.presto_data_process.row_processor import PrestoRowProcessor


class TestPrestoRowProcessor(TestCase):
    def test_given_raw_row_with_few_columns_when_process_row_should_return_processed_tuple(self):
        raw_row_data = [2, 44, 'ofek', 0.25]
        mocked_cell_processor = MagicMock(
            spec=PrestoCellProcessor,
            process_raw_cell=lambda value: value*2
        )

        presto_row_processor = PrestoRowProcessor(
            [mocked_cell_processor, mocked_cell_processor, mocked_cell_processor, mocked_cell_processor])

        expected_processed_row = (4, 88, 'ofekofek', 0.5)

        self.assertEqual(
            presto_row_processor.process_row(raw_row_data),
            expected_processed_row
        )

    def test_given_raw_row_with_mismatch_column_count_when_process_row_should_raise_data_error(self):
        raw_row_data = [2, 44, 'ofek', 0.25]

        presto_row_processor = PrestoRowProcessor([])

        self.assertRaises(DataError, presto_row_processor.process_row, raw_row_data)
