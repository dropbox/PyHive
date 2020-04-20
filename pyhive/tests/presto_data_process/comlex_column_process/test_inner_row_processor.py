from unittest import TestCase
from mock import MagicMock
from pyhive.presto_data_process.cell_processor import PrestoCellProcessor
from pyhive.presto_data_process.comlex_column_process.inner_row_processor import PrestoInnerRowProcessor


class TestPrestoInnerRowProcessor(TestCase):
    def test_given_none_cell_when_process_cell_should_return_none(self):
        self.assertIsNone(
            PrestoInnerRowProcessor([], []).process_raw_cell(None)
        )

    def test_given_inner_row_cell_when_process_cell_should_return_the_expected_processed_inner_row(self):
        mocked_value_cell_processor = MagicMock(
            spec=PrestoCellProcessor,
            process_raw_cell=lambda v: 2 * v
        )

        inner_row_map_cell = [2, "ofek", 3]

        presto_inner_row_processor = PrestoInnerRowProcessor(
            inner_column_names=["count_of_something", "name_of_someone", "another_integer"],
            inner_columns_processors=[mocked_value_cell_processor, mocked_value_cell_processor,
                                      mocked_value_cell_processor]
        )

        expected_processed_row_cell = {
            "count_of_something": 4,
            "name_of_someone": "ofekofek",
            "another_integer": 6
        }

        self.assertEqual(
            expected_processed_row_cell,
            presto_inner_row_processor.process_raw_cell(inner_row_map_cell)
        )
