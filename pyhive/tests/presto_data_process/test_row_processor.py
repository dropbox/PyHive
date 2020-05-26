from unittest import TestCase
from pyhive.exc import DataError
from pyhive.presto_data_process.row_processor import PrestoRowProcessor


class TestPrestoRowProcessor(TestCase):
    def test_given_none_when_equal_should_return_false(self):
        self.assertNotEqual(None, PrestoRowProcessor(None))
        self.assertNotEqual(PrestoRowProcessor(None), None)

    def test_given_list_when_equal_should_return_false(self):
        self.assertNotEqual([], PrestoRowProcessor(None))
        self.assertNotEqual(PrestoRowProcessor(None), [])

    def test_given_raw_row_with_few_columns_when_process_row_should_return_processed_tuple(self):
        raw_row_data = [2, 44, 'ofek', 0.25]

        def mocked_cell_processor(v):
            return v * 2

        presto_row_processor = PrestoRowProcessor(
            [mocked_cell_processor, mocked_cell_processor, mocked_cell_processor,
             mocked_cell_processor])

        expected_processed_row = (4, 88, 'ofekofek', 0.5)

        self.assertEqual(
            presto_row_processor.process_row(raw_row_data),
            expected_processed_row
        )

    def test_given_raw_row_with_mismatch_column_count_when_process_row_should_raise_data_error(
            self):
        raw_row_data = [2, 44, 'ofek', 0.25]

        presto_row_processor = PrestoRowProcessor([])

        self.assertRaises(DataError, presto_row_processor.process_row, raw_row_data)
