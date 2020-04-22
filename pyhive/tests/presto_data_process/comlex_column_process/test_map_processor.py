from unittest import TestCase
from mock import MagicMock
from pyhive.presto_data_process.cell_processor import PrestoCellProcessor
from pyhive.presto_data_process.comlex_column_process.map_processor import PrestoMapProcessor


class TestPrestoMapProcessor(TestCase):
    def test_given_none_when_equals_should_return_false(self):
        self.assertNotEqual(None, PrestoMapProcessor(None))
        self.assertNotEqual(PrestoMapProcessor(None), None)

    def test_given_dictionary_when_equals_should_return_false(self):
        self.assertNotEqual({}, PrestoMapProcessor(None))
        self.assertNotEqual(PrestoMapProcessor(None), {})

    def test_given_none_cell_when_process_cell_should_return_none(self):
        mocked_cell_processor = MagicMock(
            spec=PrestoCellProcessor
        )

        self.assertIsNone(
            PrestoMapProcessor(mocked_cell_processor).process_raw_cell(None)
        )

    def test_given_map_cell_when_process_cell_should_return_the_expected_processed_map(self):
        mocked_value_cell_processor = MagicMock(
            spec=PrestoCellProcessor,
            process_raw_cell=lambda v: 2 * v
        )

        raw_map_cell = {
            "someKey1": 2,
            "someKey2": 3,
            "key3": 4
        }
        expected_processed_map = {
            "someKey1": 4,
            "someKey2": 6,
            "key3": 8
        }

        presto_map_processor = PrestoMapProcessor(mocked_value_cell_processor)

        self.assertEqual(
            expected_processed_map,
            presto_map_processor.process_raw_cell(raw_map_cell)
        )
