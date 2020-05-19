import pyhive.presto_data_process.complex_column_process.array_processor as array_processor

from unittest import TestCase
from mock import MagicMock


class TestPrestoArrayProcessor(TestCase):
    def test_given_none_cell_when_process_cell_should_return_none(self):
        mocked_cell_processor = MagicMock()

        process_raw_cell = array_processor.new_process_raw_cell_function(mocked_cell_processor)
        self.assertIsNone(
            process_raw_cell(None)
        )

    def test_given_array_cell_when_process_cell_should_return_the_expected_processed_array(self):
        def mocked_cell_processor(v):
            return v * 2

        raw_array_cell = [1, 2, 3, 4]
        expected_processed_array = [2, 4, 6, 8]

        process_raw_cell = array_processor.new_process_raw_cell_function(mocked_cell_processor)

        self.assertEqual(
            expected_processed_array,
            process_raw_cell(raw_array_cell)
        )
