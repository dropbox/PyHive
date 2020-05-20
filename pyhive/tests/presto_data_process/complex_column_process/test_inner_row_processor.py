from unittest import TestCase
from pyhive.presto_data_process.complex_column_process.inner_row_processor import \
    new_inner_row_process_function


class TestPrestoInnerRowProcessor(TestCase):
    def test_given_none_cell_when_process_cell_should_return_none(self):
        self.assertIsNone(
            new_inner_row_process_function([], [])(None)
        )

    def test_given_inner_row_cell_when_process_cell_should_return_the_expected_processed_inner_row(
            self):
        def mocked_value_cell_processor(v):
            return v * 2

        inner_row_map_cell = [2, "ofek", 3]

        process_raw_cell = new_inner_row_process_function(
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
            process_raw_cell(inner_row_map_cell)
        )
