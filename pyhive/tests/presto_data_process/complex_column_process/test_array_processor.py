from unittest import TestCase
from mock import MagicMock
from pyhive.presto_data_process.cell_processor import PrestoCellProcessor
from pyhive.presto_data_process.complex_column_process.array_processor import PrestoArrayProcessor


class TestPrestoArrayProcessor(TestCase):
    def test_given_none_when_equals_should_return_false(self):
        self.assertNotEqual(None, PrestoArrayProcessor(None))
        self.assertNotEqual(PrestoArrayProcessor(None), None)

    def test_given_dictionary_when_equals_should_return_false(self):
        self.assertNotEqual({}, PrestoArrayProcessor(None))
        self.assertNotEqual(PrestoArrayProcessor(None), {})

    def test_given_none_cell_when_process_cell_should_return_none(self):
        mocked_cell_processor = MagicMock(
            spec=PrestoCellProcessor
        )

        self.assertIsNone(
            PrestoArrayProcessor(mocked_cell_processor).process_raw_cell(None)
        )

    def test_given_array_cell_when_process_cell_should_return_the_expected_processed_array(self):
        mocked_value_cell_processor = MagicMock(
            spec=PrestoCellProcessor,
            process_raw_cell=lambda v: 2 * v
        )

        raw_array_cell = [1, 2, 3, 4]
        expected_processed_array = [2, 4, 6, 8]

        presto_array_processor = PrestoArrayProcessor(mocked_value_cell_processor)

        self.assertEqual(
            expected_processed_array,
            presto_array_processor.process_raw_cell(raw_array_cell)
        )
